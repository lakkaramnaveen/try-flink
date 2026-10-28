"""Local control panel for the Flink/Kafka click-stream playground.

Starts and stops the Docker stack, submits the Flink SQL job, runs the traffic simulator,
and serves the dashboard at http://localhost:5000.  See docs/architecture.md.
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
import urllib.request
from pathlib import Path

from flask import Flask, Response, jsonify, request, send_file

from traffic_metrics import read_recent


ROOT = Path(__file__).resolve().parent
CONTROL_FILE = ROOT / ".simulation-control.json"
METRICS_FILE = ROOT / ".traffic-metrics.bin"
SQL_LOG_FILE = ROOT / ".flink-sql.log"
SQL_FILE = "/opt/flink/sql/clicks-per-minute.sql"
FLINK_URL = os.environ.get("FLINK_URL", "http://127.0.0.1:8081")
# Keep in sync with the <select id="speed"> options in control-panel.html.
SPEEDS = (1, 60, 120, 1000, 5000, 10000)
app = Flask(__name__)

lock = threading.RLock()
simulator: subprocess.Popen | None = None
sql_client: subprocess.Popen | None = None
compose_up: subprocess.Popen | None = None
# Bumped by every stop so an in-flight boot knows it has been cancelled.
generation = 0
state = {"running": False, "starting": False, "paused": False, "speed": 120, "message": "Ready"}


class BadRequest(ValueError):
    pass


@app.errorhandler(BadRequest)
def bad_request(error):
    return jsonify({"error": str(error)}), 400


def json_body():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        raise BadRequest("Expected a JSON object")
    return body


def save_control():
    CONTROL_FILE.write_text(json.dumps({"paused": state["paused"], "speed": state["speed"]}))


def alive(process):
    return process is not None and process.poll() is None


def run_command(args, stdout=subprocess.DEVNULL, **kwargs):
    return subprocess.Popen(args, cwd=ROOT, stdout=stdout, stderr=subprocess.STDOUT, **kwargs)


def fetch_flink(path, timeout=1):
    with urllib.request.urlopen(FLINK_URL + path, timeout=timeout) as response:
        return json.load(response)


def wait_for_flink(boot_generation, timeout=45):
    deadline = time.time() + timeout
    while time.time() < deadline and boot_generation == generation:
        try:
            fetch_flink("/overview")
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
        state.update(running=True, starting=True, paused=False, message="Starting Docker services…")
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
        compose_up = run_command(["docker", "compose", "up", "-d"])
    if compose_up.wait() != 0:
        return fail("Could not start Docker services (is Docker Desktop running?)")
    if not wait_for_flink(boot_generation):
        return fail("Flink did not become ready")

    with lock:
        if cancelled():
            return
        state["message"] = "Submitting Flink SQL…"
        # Keep the SQL client's output so query results and errors can be read with `tail -f .flink-sql.log`.
        with SQL_LOG_FILE.open("w") as log:
            sql_client = run_command(["docker", "compose", "exec", "-T", "jobmanager", "./bin/sql-client.sh", "-f", SQL_FILE], stdout=log)
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
        subprocess.run(["docker", "compose", "down", "--remove-orphans"], cwd=ROOT, check=False)
        state.update(running=False, starting=False, paused=False, message="Stopped")
        save_control()


def flink_job_summary():
    """Summarize the newest running Flink job: parallelism, throughput, and where time is spent."""
    jobs = [job for job in fetch_flink("/jobs/overview")["jobs"] if job["state"] == "RUNNING"]
    if not jobs:
        return {"state": "NO_JOB"}
    job = max(jobs, key=lambda job: job["start-time"])
    vertices = fetch_flink(f"/jobs/{job['jid']}")["vertices"]

    def metrics(vertex):
        names = "numRecordsOutPerSecond,busyTimeMsPerSecond,backPressuredTimeMsPerSecond"
        values = fetch_flink(f"/jobs/{job['jid']}/vertices/{vertex['id']}/subtasks/metrics?get={names}")
        return {metric["id"]: metric for metric in values}

    vertex_metrics = [(vertex, metrics(vertex)) for vertex in vertices]
    sources = [(vertex, values) for vertex, values in vertex_metrics if vertex["name"].startswith("Source:")]

    def worst(name):
        # Milliseconds per second → percent of the busiest subtask.  NaN is reported while a task is idle.
        readings = [values[name]["max"] for _, values in vertex_metrics if name in values]
        return round(max((value for value in readings if value == value), default=0) / 10)

    return {
        "state": job["state"],
        "name": job["name"],
        "parallelism": max((vertex["parallelism"] for vertex, _ in sources), default=0),
        "records_per_second": round(sum(values.get("numRecordsOutPerSecond", {}).get("sum", 0) for _, values in sources)),
        "busy_percent": worst("busyTimeMsPerSecond"),
        "backpressure_percent": worst("backPressuredTimeMsPerSecond"),
    }


@app.get("/")
def index():
    return send_file(ROOT / "control-panel.html")


@app.get("/api/status")
def status():
    with lock:
        if state["running"] and not state["starting"] and not alive(simulator):
            state["running"] = False
            state["message"] = "Simulation finished"
        return jsonify(state)


@app.get("/api/traffic")
def traffic_metrics():
    return Response(
        read_recent(METRICS_FILE),
        mimetype="application/octet-stream",
        headers={"Cache-Control": "no-store"},
    )


@app.get("/api/flink")
def flink():
    try:
        return jsonify(flink_job_summary())
    except (OSError, ValueError, KeyError):
        return jsonify({"state": "UNAVAILABLE"})


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
    value = json_body().get("speed", 120)
    if isinstance(value, bool) or value not in SPEEDS:
        raise BadRequest("Speed must be one of " + ", ".join(map(str, SPEEDS)))
    with lock:
        state["speed"] = int(value)
        save_control()
    return status()


if __name__ == "__main__":
    # Registered here rather than at import so that importing this module (e.g. from tests) never tears down Docker.
    atexit.register(stop_stack)
    # atexit doesn't run on SIGTERM by default, so turn it into a normal exit.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    app.run(host="127.0.0.1", port=5000, debug=False)
