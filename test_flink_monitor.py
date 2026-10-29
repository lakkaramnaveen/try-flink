"""FlinkMonitor checks against canned Flink REST responses."""

import unittest
import urllib.parse
from unittest.mock import patch

from flink_monitor import FlinkMonitor


def metric(name, max_value=0, total=0):
    return {"id": name, "min": 0, "max": max_value, "avg": 0, "sum": total}


SOURCE_METRICS = [
    metric("numRecordsOut", total=0), metric("busyTimeMsPerSecond", 250),
    {"id": "backPressuredTimeMsPerSecond", "min": "NaN", "max": "NaN", "avg": "NaN", "sum": "NaN"},  # As Flink sends it.
    metric("Source__youtube_inbound_stream[1].pendingRecords", total=1234),
    metric("Source__youtube_inbound_stream[1].numRecordsOut"),  # Not wanted: only the vertex-level count.
]
WINDOW_METRICS = [
    metric("numRecordsOut", total=9), metric("busyTimeMsPerSecond", 600),
    metric("backPressuredTimeMsPerSecond", 100),
    metric("GroupWindowAggregate[4].numLateRecordsDropped", total=17),
]


class FakeFlink:
    """Answers the REST paths FlinkMonitor uses, recording each request."""

    def __init__(self):
        self.records_out = 1000
        self.requests = []
        self.jobs = [
            {"jid": "old", "name": "old", "state": "RUNNING", "start-time": 1},
            {"jid": "new", "name": "clicks-per-minute", "state": "RUNNING", "start-time": 2},
            {"jid": "done", "name": "done", "state": "FINISHED", "start-time": 3},
        ]

    def __call__(self, path, body=None):
        self.requests.append(path)
        base, _, query = path.partition("?get=")
        # Like Flink, ignore metric names whose brackets weren't URL-encoded.
        if "[" in query:
            return []
        query = urllib.parse.unquote(query)
        vertex_metrics = {"src": SOURCE_METRICS, "agg": WINDOW_METRICS}
        if base == "/overview":
            return {"taskmanagers": 1, "slots-total": 8}
        if base == "/jobs/overview":
            return {"jobs": self.jobs}
        if base == "/jobs/new":
            return {"vertices": [
                {"id": "src", "name": "Source: youtube_inbound_stream", "parallelism": 4},
                {"id": "agg", "name": "GroupWindowAggregate", "parallelism": 4},
            ]}
        vertex = base.split("/")[4]
        values = vertex_metrics[vertex]
        if not query:
            return [{"id": value["id"]} for value in values]
        wanted = query.split(",")
        values = [value for value in values if value["id"] in wanted]
        if vertex == "src":
            values = [metric("numRecordsOut", total=self.records_out) if value["id"] == "numRecordsOut" else value for value in values]
        return values


class FlinkMonitorTests(unittest.TestCase):
    def setUp(self):
        self.flink = FakeFlink()
        self.now = 100.0
        self.monitor = FlinkMonitor("http://flink", clock=lambda: self.now)
        patcher = patch.object(self.monitor, "fetch", side_effect=self.flink)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_summary_of_the_newest_running_job(self):
        self.assertEqual(self.monitor.summary(), {
            "state": "RUNNING", "jid": "new", "name": "clicks-per-minute", "taskmanagers": 1, "slots_total": 8,
            "parallelism": 4, "records_out": 1000, "busy_percent": 60, "backpressure_percent": 10,
            "pending_records": 1234, "late_records_dropped": 17,
        })

    def test_metric_names_are_relisted_every_10_seconds(self):
        def listings():
            return len([path for path in self.flink.requests if path.endswith("/subtasks/metrics")])

        with patch("flink_monitor.time.monotonic", side_effect=[0, 0, 5, 5, 11, 11]):
            self.monitor.summary()
            self.monitor.summary()
            self.assertEqual(listings(), 2)  # Once per vertex.
            # e.g. the window operator registers numLateRecordsDropped only once it starts.
            self.monitor.summary()
        self.assertEqual(listings(), 4)

    def test_records_per_second_comes_from_the_counter_difference(self):
        self.assertEqual(self.monitor.sample()["records_per_second"], 0)  # No previous sample yet.
        self.now, self.flink.records_out = 102.0, 1500
        self.assertEqual(self.monitor.sample()["records_per_second"], 250)
        # Flink hasn't refreshed its metrics yet: the 5s window still shows the true rate, not 0.
        self.now = 103.0
        self.assertEqual(self.monitor.sample()["records_per_second"], 167)
        # Samples older than 5s drop out: the rate is now measured from the 103s sample (1500 records).
        self.now, self.flink.records_out = 108.0, 3000
        self.assertEqual(self.monitor.sample()["records_per_second"], 300)
        with patch.object(self.monitor, "start_sampling"):
            self.assertEqual([row[:2] for row in self.monitor.snapshot()["history"]], [[100.0, 0], [102.0, 250], [103.0, 167], [108.0, 300]])

    def test_no_job_and_unreachable_flink(self):
        self.flink.jobs = []
        self.assertEqual(self.monitor.sample(), {"state": "NO_JOB", "taskmanagers": 1, "slots_total": 8})
        with patch.object(self.monitor, "fetch", side_effect=OSError("connection refused")):
            self.assertEqual(self.monitor.sample(), {"state": "UNAVAILABLE"})

    def test_running_job_by_name(self):
        self.assertEqual(self.monitor.running_job("old")["jid"], "old")
        self.assertIsNone(self.monitor.running_job("missing"))

    def test_stop_with_savepoint_waits_for_the_location(self):
        responses = iter([
            {"request-id": "trigger"},
            {"status": {"id": "IN_PROGRESS"}},
            {"status": {"id": "COMPLETED"}, "operation": {"location": "file:/flink-data/savepoints/sp-1"}},
        ])
        with patch.object(self.monitor, "fetch", side_effect=lambda *args, **kwargs: next(responses)) as fetch, \
                patch("flink_monitor.time.sleep"):
            self.assertEqual(self.monitor.stop_with_savepoint("job"), "file:/flink-data/savepoints/sp-1")
        self.assertEqual(fetch.call_args_list[0].args, ("/jobs/job/stop",))
        self.assertEqual(fetch.call_args_list[0].kwargs, {"body": {"drain": False}})

    def test_failed_savepoint_raises(self):
        responses = iter([
            {"request-id": "trigger"},
            {"status": {"id": "COMPLETED"}, "operation": {"failure-cause": {"stack-trace": "java.lang.Exception: no space\n\tat ..."}}},
        ])
        with patch.object(self.monitor, "fetch", side_effect=lambda *args, **kwargs: next(responses)):
            with self.assertRaisesRegex(RuntimeError, "no space"):
                self.monitor.stop_with_savepoint("job")


if __name__ == "__main__":
    unittest.main()
