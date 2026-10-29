"""End-to-end checks against the real Docker stack: Flink counts every click exactly once, even across a
rescale or a crashed TaskManager.

Needs Docker, so it only runs with RUN_E2E=1.  `make e2e` starts a fresh stack and runs them (~5 minutes).
"""

import csv
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SPEED = 1000  # The day of clicks takes ~90s, long enough to rescale in the middle.
DISRUPT_AFTER_SECONDS = 30  # When to rescale or crash, while clicks are flowing.


def expected_counts():
    """Clicks per (videoid, minute) straight from the CSV."""
    with open(ROOT / "youtube-clicks.csv", newline="") as source:
        return Counter((row["videoid"], row["timestamp"][:16] + ":00") for row in csv.DictReader(source))


def read_results(bootstrap_servers, quiet_seconds=15):
    """Every row of the output topic, keeping the latest count per (videoid, minute) like the dashboard does."""
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"e2e-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(["clicks-per-minute"])
    counts, rows, last_message = {}, 0, time.monotonic()
    try:
        while time.monotonic() - last_message < quiet_seconds:
            message = consumer.poll(0.5)
            if message is None or message.error():
                continue
            row = json.loads(message.value())
            counts[(row["videoid"], row["window_start"][:19])] = row["click_count"]
            rows += 1
            last_message = time.monotonic()
    finally:
        consumer.close()
    return counts, rows


def docker_compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, capture_output=True)


@unittest.skipUnless(os.environ.get("RUN_E2E"), "needs the Docker stack: run `make e2e`")
class EndToEndTests(unittest.TestCase):
    def setUp(self):
        # Each test starts from empty topics and no job, so its counts are its own.
        docker_compose("down", "-v", "--remove-orphans")
        docker_compose("up", "-d", "--build", "--wait", "jobmanager", "taskmanager")
        import app
        self.app = app
        for _ in range(60):
            try:
                if app.monitor.fetch("/overview")["taskmanagers"]:
                    break
            except OSError:
                pass
            time.sleep(1)

    def start_traffic(self):
        """Submit the job at parallelism 4 and replay the day of clicks at SPEED."""
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        control = Path(directory.name) / "control.json"
        control.write_text(json.dumps({"paused": False, "speed": SPEED}))
        self.assertEqual(self.app.submit_job(4).wait(), 0, "Job submission failed, see .flink-sql.log")
        simulator = subprocess.Popen(
            [sys.executable, "run-sim.py"], cwd=ROOT, stdout=subprocess.DEVNULL,
            env={**os.environ, "SIM_CONTROL_FILE": str(control), "SIM_METRICS_FILE": str(Path(directory.name) / "metrics.bin")},
        )
        self.addCleanup(simulator.kill)
        time.sleep(DISRUPT_AFTER_SECONDS)
        self.assertIsNone(simulator.poll(), "The simulation finished too early; lower SPEED")
        job = self.app.monitor.running_job(self.app.JOB_NAME)
        self.assertIsNotNone(job, "The job isn't running")
        return simulator, job

    def test_every_click_is_counted_exactly_once_across_a_rescale(self):
        simulator, job = self.start_traffic()
        # Rescale 4 → 2 while clicks are flowing: stop with a savepoint, resubmit from it.
        savepoint = self.app.monitor.stop_with_savepoint(job["jid"])
        self.assertEqual(self.app.submit_job(2, savepoint).wait(), 0, "Resubmission failed, see .flink-sql.log")
        self.assertNotEqual(self.app.monitor.running_job(self.app.JOB_NAME)["jid"], job["jid"])
        self.assertEqual(simulator.wait(timeout=600), 0)
        self.assert_counts_match()

    def test_every_click_is_counted_exactly_once_after_a_taskmanager_crash(self):
        simulator, job = self.start_traffic()
        # Kill the TaskManager outright (no graceful shutdown), then bring it back.  Flink restarts the job
        # from its last checkpoint: open windows and Kafka offsets as of then.  Rows written after that
        # checkpoint are written again, which the reader's "latest count wins" absorbs.
        docker_compose("kill", "taskmanager")
        docker_compose("start", "taskmanager")
        self.assertEqual(simulator.wait(timeout=600), 0)
        restarts = self.app.monitor.fetch(f"/jobs/{job['jid']}/exceptions").get("exceptionHistory", {}).get("entries", [])
        self.assertTrue(restarts, "Flink never noticed the crash, so this tested nothing")
        self.assert_counts_match(expect_repeats=True)

    def assert_counts_match(self, expect_repeats=False):
        actual, rows = read_results(self.app.KAFKA_BOOTSTRAP_SERVERS)

        # The last minute never closes: no click after it advances the watermark past its end.
        expected = expected_counts()
        last_minute = max(minute for _, minute in expected)
        expected = {key: count for key, count in expected.items() if key[1] != last_minute}
        missing = {key: count for key, count in expected.items() if key not in actual}
        wrong = {key: (actual[key], count) for key, count in expected.items() if key in actual and actual[key] != count}
        extra = set(actual) - set(expected) - {key for key in actual if key[1] == last_minute}
        print(f"\n{len(expected):,} (video, minute) counts expected, {rows:,} rows read, "
              f"{rows - len(actual):,} written more than once", file=sys.stderr)
        self.assertEqual((len(missing), len(wrong), len(extra)), (0, 0, 0),
                         f"missing {list(missing.items())[:5]}, wrong (got, expected) {list(wrong.items())[:5]}, extra {list(extra)[:5]}")
        self.assertEqual(sum(actual[key] for key in expected), sum(expected.values()))
        if expect_repeats:
            self.assertGreater(rows, len(actual), "No rows were rewritten, so the recovery wasn't exercised")


if __name__ == "__main__":
    unittest.main()
