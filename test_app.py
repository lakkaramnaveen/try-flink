"""Control panel API checks with Docker, Flink, and the simulator replaced by fakes."""

import os
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import app


class FakeProcess:
    def __init__(self, returncode=0, wait_gate=None):
        self.returncode = returncode
        self.wait_gate = wait_gate
        self.terminated = False

    def poll(self):
        return None if self.wait_gate and not self.wait_gate.is_set() else self.returncode

    def wait(self, timeout=None):
        if self.wait_gate:
            self.wait_gate.wait(timeout)
        return self.returncode

    def terminate(self):
        self.terminated = True
        if self.wait_gate:
            self.wait_gate.set()

    def kill(self):
        self.terminate()


class AppTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        for name, value in {
            "CONTROL_FILE": Path(directory.name) / "control.json",
            "SQL_LOG_FILE": Path(directory.name) / "sql.log",
            "state": {"running": False, "starting": False, "paused": False, "speed": 120, "message": "Ready"},
            "simulator": None,
            "sql_client": None,
            "compose_up": None,
        }.items():
            patcher = patch.object(app, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.down = patch("app.subprocess.run")
        self.down.start()
        self.addCleanup(self.down.stop)
        self.client = app.app.test_client()

    def test_importing_the_app_never_runs_docker(self):
        # A fake `docker` on PATH records any call, e.g. a teardown registered at import time.
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "called"
            fake_docker = Path(directory) / "docker"
            fake_docker.write_text(f"#!/bin/sh\ntouch {marker}\n")
            fake_docker.chmod(0o755)
            env = {**os.environ, "PATH": f"{directory}{os.pathsep}{os.environ['PATH']}"}
            subprocess.run([sys.executable, "-c", "import app"], cwd=Path(app.__file__).parent, env=env, check=True)
            self.assertFalse(marker.exists())

    def test_speed_rejects_bad_input_with_400(self):
        for body in ({"speed": 7}, {"speed": "fast"}, {"speed": True}, [], None):
            with self.subTest(body=body):
                response = self.client.post("/api/speed", json=body)
                self.assertEqual(response.status_code, 400)
                self.assertIn("error", response.json)
        self.assertEqual(self.client.post("/api/speed", data="not json").status_code, 400)

    def test_speed_and_pause_are_saved_for_the_simulator(self):
        self.assertEqual(self.client.post("/api/speed", json={"speed": 1000}).json["speed"], 1000)
        self.assertEqual(self.client.post("/api/traffic", json={"paused": True}).json["paused"], True)
        self.assertEqual(app.CONTROL_FILE.read_text(), '{"paused": true, "speed": 1000}')
        self.assertEqual(self.client.post("/api/traffic", json={"paused": "yes"}).status_code, 400)

    def test_start_returns_immediately_and_status_stays_responsive(self):
        docker_up_finished = threading.Event()
        commands = []

        def fake_run_command(args, **kwargs):
            commands.append(args[:3])
            return FakeProcess(wait_gate=docker_up_finished if args[:3] == ["docker", "compose", "up"] else None)

        with patch("app.run_command", side_effect=fake_run_command), patch("app.wait_for_flink", return_value=True):
            response = self.client.post("/api/start")
            self.assertTrue(response.json["starting"])
            # The lock isn't held during boot, so polling answers while Docker is still starting.
            self.assertTrue(self.client.get("/api/status").json["running"])

            docker_up_finished.set()
            for _ in range(100):
                if not app.state["starting"]:
                    break
                threading.Event().wait(0.01)
        self.assertEqual(app.state["message"], "Traffic is running")
        self.assertEqual(commands, [["docker", "compose", "up"], ["docker", "compose", "exec"], ["python3", "run-sim.py"]])

    def test_stop_during_boot_cancels_it(self):
        docker_up = FakeProcess(returncode=-15, wait_gate=threading.Event())
        with patch("app.run_command", return_value=docker_up) as run_command:
            self.client.post("/api/start")
            for _ in range(100):
                if app.compose_up is not None:
                    break
                threading.Event().wait(0.01)
            self.assertEqual(self.client.post("/api/stop").json["message"], "Stopped")
        self.assertTrue(docker_up.terminated)
        self.assertEqual(run_command.call_count, 1)
        self.assertFalse(app.state["running"])

    def test_failed_docker_start_is_reported(self):
        with patch("app.run_command", return_value=FakeProcess(returncode=1)):
            app.boot_stack(app.generation)
        self.assertFalse(app.state["running"])
        self.assertIn("Docker", app.state["message"])

    def test_flink_summary_reports_source_rate_and_worst_subtask(self):
        def metric(name, max_value, total=0):
            return {"id": name, "max": max_value, "sum": total}

        responses = {
            "/jobs/overview": {"jobs": [
                {"jid": "old", "name": "old", "state": "RUNNING", "start-time": 1},
                {"jid": "new", "name": "clicks-per-minute", "state": "RUNNING", "start-time": 2},
                {"jid": "done", "name": "done", "state": "FINISHED", "start-time": 3},
            ]},
            "/jobs/new": {"vertices": [
                {"id": "src", "name": "Source: youtube_inbound_stream", "parallelism": 4},
                {"id": "agg", "name": "GroupWindowAggregate", "parallelism": 4},
            ]},
            "/jobs/new/vertices/src/subtasks/metrics": [
                metric("numRecordsOutPerSecond", 400, 1500.4), metric("busyTimeMsPerSecond", 250),
                metric("backPressuredTimeMsPerSecond", float("nan")),
            ],
            "/jobs/new/vertices/agg/subtasks/metrics": [
                metric("numRecordsOutPerSecond", 9, 9), metric("busyTimeMsPerSecond", 600),
                metric("backPressuredTimeMsPerSecond", 100),
            ],
        }
        with patch("app.fetch_flink", side_effect=lambda path: responses[path.split("?")[0]]):
            summary = self.client.get("/api/flink").json
        self.assertEqual(summary, {
            "state": "RUNNING", "name": "clicks-per-minute", "parallelism": 4,
            "records_per_second": 1500, "busy_percent": 60, "backpressure_percent": 10,
        })

    def test_flink_summary_without_flink(self):
        with patch("app.fetch_flink", side_effect=OSError("connection refused")):
            self.assertEqual(self.client.get("/api/flink").json, {"state": "UNAVAILABLE"})
        with patch("app.fetch_flink", return_value={"jobs": []}):
            self.assertEqual(self.client.get("/api/flink").json, {"state": "NO_JOB"})


if __name__ == "__main__":
    unittest.main()
