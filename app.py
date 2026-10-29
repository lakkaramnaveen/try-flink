"""Local control panel for the Flink/Kafka click-stream playground.

Starts and stops the Docker stack, submits the Flink SQL job, runs the traffic simulator,
and serves the dashboard at http://localhost:5050.  See docs/architecture.md.
"""

from __future__ import annotations

import atexit
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from flask import Flask, Response, jsonify, request, send_file

from flink_monitor import FlinkMonitor
from results import ResultsConsumer, TopVideos
from traffic_metrics import read_recent


ROOT = Path(__file__).resolve().parent
CONTROL_FILE = ROOT / ".simulation-control.json"
METRICS_FILE = ROOT / ".traffic-metrics.bin"
SQL_LOG_FILE = ROOT / ".flink-sql.log"
# Per-submission settings (parallelism, savepoint), passed to the SQL client with -i.  ./sql is mounted into the jobmanager.
SESSION_FILE = ROOT / "sql" / ".session.sql"
SQL_FILE = "/opt/flink/sql/clicks-per-minute.sql"
JOB_NAME = "clicks-per-minute"
FLINK_URL = os.environ.get("FLINK_URL", "http://127.0.0.1:8081")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092")
# Not 5000: macOS AirPlay Receiver listens there on all interfaces, so http://localhost:5000 reaches it instead.
PORT = int(os.environ.get("PORT", 5050))
# Keep these in sync with the <select> options in control-panel.html.
SPEEDS = (1, 60, 120, 1000, 5000, 10000)
PARALLELISMS = (1, 2, 4, 8)
LATE_PERCENTS = (0, 5, 20)
BURST_SECONDS = 30
BURST_FACTOR = 10
app = Flask(__name__)

lock = threading.RLock()
simulator: subprocess.Popen | None = None
sql_client: subprocess.Popen | None = None
compose_up: subprocess.Popen | None = None
# Bumped by every stop so an in-flight boot knows it has been cancelled.
generation = 0
state = {
    "running": False, "starting": False, "rescaling": False, "paused": False, "speed": 120,
    "parallelism": 4, "late_percent": 0, "burst_until": 0, "message": "Ready",
}
monitor = FlinkMonitor(FLINK_URL)
top_videos = TopVideos()
results_consumer = ResultsConsumer(top_videos, KAFKA_BOOTSTRAP_SERVERS)


class BadRequest(ValueError):
    pass


class Conflict(RuntimeError):
    pass


@app.errorhandler(BadRequest)
def bad_request(error):
    return jsonify({"error": str(error)}), 400


@app.errorhandler(Conflict)
def conflict(error):
    return jsonify({"error": str(error)}), 409


def json_body():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        raise BadRequest("Expected a JSON object")
    return body


def choice(body, key, allowed):
    value = body.get(key)
    if isinstance(value, bool) or value not in allowed:
        raise BadRequest(f"{key} must be one of " + ", ".join(map(str, allowed)))
    return int(value)


def save_control():
    """Write the settings the simulator polls for.  See read_settings() in run-sim.py."""
    CONTROL_FILE.write_text(json.dumps({
        "paused": state["paused"],
        "speed": state["speed"],
        "late_percent": state["late_percent"],
        "burst_until": state["burst_until"],
        "burst_factor": BURST_FACTOR,
    }))


def alive(process):
    return process is not None and process.poll() is None


def run_command(args, stdout=subprocess.DEVNULL, **kwargs):
    return subprocess.Popen(args, cwd=ROOT, stdout=stdout, stderr=subprocess.STDOUT, **kwargs)


def submit_job(parallelism, savepoint=None):
    """Submit the SQL job.  It's an INSERT, so the SQL client exits once the job is running."""
    settings = [f"SET 'parallelism.default' = '{parallelism}';"]
    if savepoint:
        settings.append(f"SET 'execution.savepoint.path' = '{savepoint}';")
    SESSION_FILE.write_text("\n".join(settings) + "\n")
    # Keep the SQL client's output so submission errors can be read in .flink-sql.log.
    with SQL_LOG_FILE.open("a") as log:
        return run_command(
            ["docker", "compose", "exec", "-T", "jobmanager", "./bin/sql-client.sh",
             "-i", "/opt/flink/sql/.session.sql", "-f", SQL_FILE],
            stdout=log,
        )


def wait_for_flink(boot_generation, timeout=45):
    deadline = time.time() + timeout
    while time.time() < deadline and boot_generation == generation:
        try:
            monitor.fetch("/overview")
            return True
        except Exception:
            time.sleep(1)
    return False


def stop_process(process):
    if alive(process):
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def start_stack():
    """Begin booting in the background so status polling stays responsive while Docker starts."""
    with lock:
        if state["running"]:
            return
        state.update(running=True, starting=True, paused=False, burst_until=0, message="Starting Docker services…")
        save_control()
        threading.Thread(target=boot_stack, args=(generation,), daemon=True).start()


def boot_stack(boot_generation):
    global simulator, sql_client, compose_up

    def cancelled():
        return boot_generation != generation

    def fail(message):
        with lock:
            if not cancelled():
                state.update(running=False, starting=False, message=message)

    with lock:
        if cancelled():
            return
        compose_up = run_command(["docker", "compose", "up", "-d", "--build"])
    if compose_up.wait() != 0:
        return fail("Could not start Docker services (is Docker Desktop running?)")
    if not wait_for_flink(boot_generation):
        return fail("Flink did not become ready")

    with lock:
        if cancelled():
            return
        state["message"] = "Submitting Flink SQL…"
        SQL_LOG_FILE.write_text("")
        results_consumer.start()
        sql_client = submit_job(state["parallelism"])
        # This simulation may start before the above FlinkSQL is submitted and that's fine because the clicks will be durably stored in Kafka either way.
        simulator = run_command(
            ["python3", "run-sim.py"],
            env={**os.environ, "SIM_CONTROL_FILE": str(CONTROL_FILE), "SIM_METRICS_FILE": str(METRICS_FILE)},
        )
        state.update(starting=False, message="Traffic is running")


def stop_stack():
    global simulator, sql_client, compose_up, generation
    with lock:
        generation += 1
        for process in (compose_up, simulator, sql_client):
            stop_process(process)
        simulator = sql_client = compose_up = None
        results_consumer.stop()
        subprocess.run(["docker", "compose", "down", "--remove-orphans"], cwd=ROOT, check=False)
        state.update(running=False, starting=False, rescaling=False, paused=False, burst_until=0, message="Stopped")
        save_control()


def rescale(parallelism, rescale_generation):
    """Stop the job with a savepoint and resubmit it from that savepoint at the new parallelism.

    The savepoint carries the open windows' counts and the Kafka offsets, so no clicks are lost or
    double counted.  Flink redistributes the state across the new number of subtasks on restore.
    """
    global sql_client
    try:
        job = monitor.running_job(JOB_NAME)
        savepoint = monitor.stop_with_savepoint(job["jid"]) if job else None
        with lock:
            if rescale_generation != generation:
                return
            sql_client = submit_job(parallelism, savepoint)
            state.update(rescaling=False, message=f"Running with parallelism {parallelism}")
    except Exception as error:
        with lock:
            if rescale_generation == generation:
                state.update(rescaling=False, message=f"Rescale failed: {error}")


@app.get("/")
def index():
    return send_file(ROOT / "control-panel.html")


@app.get("/api/status")
def status():
    with lock:
        if state["running"] and not state["starting"] and not alive(simulator):
            state["running"] = False
            state["message"] = "Simulation finished"
        return jsonify({**state, "burst_seconds_left": max(0, round(state["burst_until"] - time.time()))})


@app.get("/api/traffic")
def traffic_metrics():
    return Response(
        read_recent(METRICS_FILE),
        mimetype="application/octet-stream",
        headers={"Cache-Control": "no-store"},
    )


@app.get("/api/flink")
def flink():
    return jsonify(monitor.snapshot())


@app.get("/api/top-videos")
def top():
    return jsonify(top_videos.latest())


@app.post("/api/start")
def start():
    try:
        start_stack()
        return status()
    except OSError as error:
        state.update(running=False, starting=False, message=f"Could not start: {error}")
        return jsonify(state), 500


@app.post("/api/stop")
def stop():
    stop_stack()
    return status()


@app.post("/api/restart")
def restart():
    stop_stack()
    return start()


@app.post("/api/traffic")
def traffic():
    paused = json_body().get("paused", False)
    if not isinstance(paused, bool):
        raise BadRequest("paused must be true or false")
    with lock:
        state["paused"] = paused
        state["message"] = "Traffic is paused" if paused else "Traffic is running"
        save_control()
    return status()


@app.post("/api/speed")
def speed():
    value = choice(json_body(), "speed", SPEEDS)
    with lock:
        state["speed"] = value
        save_control()
    return status()


@app.post("/api/parallelism")
def parallelism():
    value = choice(json_body(), "parallelism", PARALLELISMS)
    with lock:
        if state["rescaling"]:
            raise Conflict("Already rescaling")
        if value == state["parallelism"]:
            return status()
        state["parallelism"] = value
        # Before the stack is up, the new value is simply used at the next start.
        if state["running"] and not state["starting"]:
            state.update(rescaling=True, message=f"Rescaling to parallelism {value}: taking a savepoint…")
            threading.Thread(target=rescale, args=(value, generation), daemon=True).start()
    return status()


@app.post("/api/burst")
def burst():
    with lock:
        if not state["running"]:
            raise Conflict("Start the traffic first")
        state["burst_until"] = time.time() + BURST_SECONDS
        save_control()
    return status()


@app.post("/api/late")
def late():
    value = choice(json_body(), "late_percent", LATE_PERCENTS)
    with lock:
        state["late_percent"] = value
        save_control()
    return status()


if __name__ == "__main__":
    # Registered here rather than at import so that importing this module (e.g. from tests) never tears down Docker.
    atexit.register(stop_stack)
    # atexit doesn't run on SIGTERM by default, so turn it into a normal exit.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    app.run(host="127.0.0.1", port=PORT, debug=False)
