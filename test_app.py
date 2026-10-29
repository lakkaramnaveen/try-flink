"""Control panel API checks with Docker, Flink, Kafka, and the simulator replaced by fakes."""

import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

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


def wait_for(condition):
    for _ in range(200):
        if condition():
            return
        threading.Event().wait(0.01)
    raise AssertionError("Timed out")


class AppTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.monitor = Mock()
        for name, value in {
            "CONTROL_FILE": self.directory / "control.json",
            "SQL_LOG_FILE": self.directory / "sql.log",
            "SESSION_FILE": self.directory / "session.sql",
            "state": {
                "running": False, "starting": False, "rescaling": False, "paused": False, "speed": 120,
                "parallelism": 4, "late_percent": 0, "burst_until": 0, "message": "Ready",
            },
            "simulator": None,
            "sql_client": None,
            "compose_up": None,
            "monitor": self.monitor,
            "results_consumer": Mock(),
        }.items():
            patcher = patch.object(app, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.down = patch("app.subprocess.run")
        self.down.start()
        self.addCleanup(self.down.stop)
        self.client = app.app.test_client()

    def control(self):
        return json.loads(app.CONTROL_FILE.read_text())

    def boot(self):
        """Start the stack with every external command faked; returns the commands run."""
        commands = []

        def fake_run_command(args, **kwargs):
            commands.append(args)
            return FakeProcess(wait_gate=threading.Event() if args[0] == "python3" else None)

        with patch("app.run_command", side_effect=fake_run_command), patch("app.wait_for_flink", return_value=True):
            self.client.post("/api/start")
            wait_for(lambda: not app.state["starting"])
        return commands

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

    def test_other_websites_cannot_control_the_panel(self):
        # A page on another site POSTing to localhost: the browser names it in Origin.
        with patch("app.stop_stack") as stop_stack:
            response = self.client.post("/api/stop", headers={"Origin": "https://evil.example"})
        self.assertEqual(response.status_code, 403)
        stop_stack.assert_not_called()
        # DNS rebinding: same origin as far as the browser knows, but the Host is the other site's.
        self.assertEqual(self.client.get("/api/status", headers={"Host": "evil.example:5050"}).status_code, 403)

    def test_the_panel_itself_is_allowed(self):
        for host in ("localhost:5050", "127.0.0.1:5050", "[::1]:5050"):
            with self.subTest(host=host):
                response = self.client.post("/api/speed", json={"speed": 60}, headers={"Host": host, "Origin": f"http://{host}"})
                self.assertEqual(response.status_code, 200)
        self.assertEqual(self.client.post("/api/speed", json={"speed": 60}).status_code, 200)  # No Origin, e.g. curl.

    def test_choices_reject_bad_input_with_400(self):
        for path, key in (("/api/speed", "speed"), ("/api/parallelism", "parallelism"), ("/api/late", "late_percent")):
            for body in ({key: 7}, {key: "fast"}, {key: True}, [], None):
                with self.subTest(path=path, body=body):
                    response = self.client.post(path, json=body)
                    self.assertEqual(response.status_code, 400)
                    self.assertIn("error", response.json)
        self.assertEqual(self.client.post("/api/speed", data="not json").status_code, 400)

    def test_pickers_offer_exactly_the_allowed_values(self):
        html = (Path(app.__file__).parent / "control-panel.html").read_text()
        for select, allowed in (("speed", app.SPEEDS), ("parallelism", app.PARALLELISMS), ("late", app.LATE_PERCENTS)):
            with self.subTest(select=select):
                options = re.search(rf'<select id="{select}".*?</select>', html, re.S).group(0)
                self.assertEqual(tuple(int(value) for value in re.findall(r'<option value="(\d+)"', options)), allowed)

    def test_settings_are_saved_for_the_simulator(self):
        self.assertEqual(self.client.post("/api/speed", json={"speed": 1000}).json["speed"], 1000)
        self.assertEqual(self.client.post("/api/traffic", json={"paused": True}).json["paused"], True)
        self.assertEqual(self.client.post("/api/late", json={"late_percent": 20}).json["late_percent"], 20)
        self.assertEqual(self.control(), {
            "paused": True, "speed": 1000, "late_percent": 20, "burst_until": 0, "burst_factor": app.BURST_FACTOR,
        })
        self.assertEqual(self.client.post("/api/traffic", json={"paused": "yes"}).status_code, 400)

    def test_burst_needs_running_traffic_and_lasts_30_seconds(self):
        self.assertEqual(self.client.post("/api/burst").status_code, 409)
        app.state["running"] = True
        with patch("app.time.time", return_value=1000.0):
            self.client.post("/api/burst")
            self.assertEqual(self.client.get("/api/status").json["burst_seconds_left"], 30)
        self.assertEqual(self.control()["burst_until"], 1030.0)

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
            wait_for(lambda: not app.state["starting"])
        self.assertEqual(app.state["message"], "Traffic is running")
        self.assertEqual(commands, [["docker", "compose", "up"], ["docker", "compose", "exec"], ["python3", "run-sim.py"]])
        app.results_consumer.start.assert_called_once()

    def test_job_is_submitted_with_the_chosen_parallelism(self):
        self.client.post("/api/parallelism", json={"parallelism": 2})
        commands = self.boot()
        self.assertIn("-i", commands[1])
        self.assertEqual(app.SESSION_FILE.read_text(), "SET 'parallelism.default' = '2';\n")

    def test_rescale_stops_with_a_savepoint_and_resubmits_from_it(self):
        self.boot()
        self.monitor.running_job.return_value = {"jid": "job-1"}
        self.monitor.stop_with_savepoint.return_value = "file:/flink-data/savepoints/savepoint-1"
        with patch("app.run_command", return_value=FakeProcess()) as run_command:
            self.assertTrue(self.client.post("/api/parallelism", json={"parallelism": 8}).json["rescaling"])
            wait_for(lambda: not app.state["rescaling"])
        self.monitor.stop_with_savepoint.assert_called_once_with("job-1")
        self.assertEqual(app.SESSION_FILE.read_text(), (
            "SET 'parallelism.default' = '8';\n"
            "SET 'execution.savepoint.path' = 'file:/flink-data/savepoints/savepoint-1';\n"
        ))
        self.assertEqual(run_command.call_count, 1)
        self.assertEqual(app.state["message"], "Running with parallelism 8")

    def test_failed_rescale_is_reported(self):
        self.boot()
        self.monitor.running_job.return_value = {"jid": "job-1"}
        self.monitor.stop_with_savepoint.side_effect = TimeoutError("Savepoint did not complete")
        self.client.post("/api/parallelism", json={"parallelism": 1})
        wait_for(lambda: not app.state["rescaling"])
        self.assertEqual(app.state["message"], "Rescale failed: Savepoint did not complete")

    def test_stop_during_boot_cancels_it(self):
        docker_up = FakeProcess(returncode=-15, wait_gate=threading.Event())
        with patch("app.run_command", return_value=docker_up) as run_command:
            self.client.post("/api/start")
            wait_for(lambda: app.compose_up is not None)
            self.assertEqual(self.client.post("/api/stop").json["message"], "Stopped")
        self.assertTrue(docker_up.terminated)
        self.assertEqual(run_command.call_count, 1)
        self.assertFalse(app.state["running"])
        app.results_consumer.stop.assert_called()

    def test_failed_docker_start_is_reported(self):
        with patch("app.run_command", return_value=FakeProcess(returncode=1)):
            app.boot_stack(app.generation)
        self.assertFalse(app.state["running"])
        self.assertIn("Docker", app.state["message"])

    def test_flink_and_top_videos_endpoints(self):
        self.monitor.snapshot.return_value = {"state": "NO_JOB", "history": []}
        self.assertEqual(self.client.get("/api/flink").json, {"state": "NO_JOB", "history": []})
        with patch.object(app.top_videos, "latest", return_value={"window_start": None, "total": 0, "videos": []}):
            self.assertEqual(self.client.get("/api/top-videos").json["videos"], [])


if __name__ == "__main__":
    unittest.main()
