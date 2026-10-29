"""Reads Flink's REST API: what the running job is doing, a once-a-second history of it, and rescaling.

Every number here comes from Flink's own metrics; see docs/architecture.md for what each one means.
"""

from __future__ import annotations

import json
import threading
import time
import traceback
import urllib.parse
import urllib.request
from collections import deque

HISTORY_SECONDS = 900  # Matches the dashboard's 15 minute chart.
# Flink refreshes metrics about once a second, the same rate we sample at, so a 1s difference is often 0 or double.
# Differencing over a few seconds smooths that out while staying far more responsive than Flink's 60s meters.
RATE_WINDOW_SECONDS = 5
# Operators register their metrics as they start, so re-list a vertex's metric names this often.
METRIC_NAMES_TTL_SECONDS = 10


class FlinkMonitor:
    def __init__(self, url, clock=time.time):
        self.url = url
        self._clock = clock
        self._metric_ids: dict[tuple[str, str], tuple[float, list[str]]] = {}  # (job, vertex) → (listed at, names)
        self._lock = threading.Lock()
        self._history: deque = deque(maxlen=HISTORY_SECONDS)
        self._latest = {"state": "UNAVAILABLE"}
        self._counts: deque = deque()  # (job id, time, records out), the last RATE_WINDOW_SECONDS of samples
        self._sampler: threading.Thread | None = None

    def fetch(self, path, body=None, timeout=1):
        data = None if body is None else json.dumps(body).encode()
        headers = {} if body is None else {"Content-Type": "application/json"}
        request = urllib.request.Request(self.url + path, data=data, headers=headers)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)

    def running_job(self, name=None):
        """The newest running job, optionally only one with the given name."""
        jobs = [
            job for job in self.fetch("/jobs/overview")["jobs"]
            if job["state"] == "RUNNING" and name in (None, job["name"])
        ]
        return max(jobs, key=lambda job: job["start-time"], default=None)

    def _vertex_metrics(self, job_id, vertex):
        """Each metric aggregated across the vertex's subtasks, as {name: {"min", "max", "avg", "sum"}}.

        Source and operator metrics are prefixed with the operator's name (e.g.
        "Source__youtube_inbound_stream[1].pendingRecords"), so they're looked up by suffix.
        """
        base = f"/jobs/{job_id}/vertices/{vertex['id']}/subtasks/metrics"
        key = (job_id, vertex["id"])
        listed_at, ids = self._metric_ids.get(key, (None, []))
        now = time.monotonic()
        if listed_at is None or now - listed_at > METRIC_NAMES_TTL_SECONDS:
            available = [metric["id"] for metric in self.fetch(base)]
            wanted = {"numRecordsOut", "busyTimeMsPerSecond", "backPressuredTimeMsPerSecond"}
            ids = [name for name in available if name in wanted or name.endswith((".pendingRecords", ".numLateRecordsDropped"))]
            self._metric_ids[key] = (now, ids)
        if not ids:
            return {}
        # Operator names contain brackets, which Flink silently ignores unless they're URL-encoded.
        values = self.fetch(f"{base}?get={urllib.parse.quote(','.join(ids), safe=',')}")
        # Aggregates are usually numbers, but an idle task's busy time comes back as the string "NaN".
        return {
            metric["id"].rsplit(".", 1)[-1]: {key: float(metric[key]) for key in ("min", "max", "avg", "sum") if key in metric}
            for metric in values
        }

    def summary(self):
        """A snapshot of the newest running job, or {"state": "NO_JOB"}."""
        overview = self.fetch("/overview")
        cluster = {"taskmanagers": overview["taskmanagers"], "slots_total": overview["slots-total"]}
        job = self.running_job()
        if job is None:
            return {"state": "NO_JOB", **cluster}
        vertices = self.fetch(f"/jobs/{job['jid']}")["vertices"]
        measured = [(vertex, self._vertex_metrics(job["jid"], vertex)) for vertex in vertices]
        sources = [values for vertex, values in measured if vertex["name"].startswith("Source:")]

        def total(name, among):
            return sum(values[name]["sum"] for values in among if name in values)

        def worst_percent(name):
            # Milliseconds per second → percent, for the worst subtask of any operator.  NaN (value != value) means idle.
            readings = [values[name]["max"] for _, values in measured if name in values]
            return round(max((value for value in readings if value == value), default=0) / 10)

        return {
            "state": "RUNNING",
            "jid": job["jid"],
            "name": job["name"],
            **cluster,
            "parallelism": max((vertex["parallelism"] for vertex, _ in measured if vertex["name"].startswith("Source:")), default=0),
            "records_out": total("numRecordsOut", sources),
            "busy_percent": worst_percent("busyTimeMsPerSecond"),
            "backpressure_percent": worst_percent("backPressuredTimeMsPerSecond"),
            # Records in Kafka the source hasn't read yet, summed over partitions: consumer lag.
            "pending_records": round(total("pendingRecords", sources)),
            # Clicks that arrived after their minute's window had already closed.
            "late_records_dropped": round(total("numLateRecordsDropped", [values for _, values in measured])),
        }

    def sample(self):
        """Take one snapshot, derive records/s from the change in records out, and add it to the history."""
        now = self._clock()
        try:
            summary = self.summary()
        except (OSError, ValueError, KeyError, TypeError):
            summary = {"state": "UNAVAILABLE"}
        if summary["state"] == "RUNNING":
            # Flink's own *PerSecond meters average over 60s, too slow for a live chart, so difference the counter.
            if self._counts and self._counts[0][0] != summary["jid"]:
                self._counts.clear()  # A new job (e.g. after a rescale) restarts its counters.
            self._counts.append((summary["jid"], now, summary["records_out"]))
            while now - self._counts[0][1] > RATE_WINDOW_SECONDS:
                self._counts.popleft()
            _, oldest_time, oldest_count = self._counts[0]
            elapsed = now - oldest_time
            summary["records_per_second"] = max(0, round((summary["records_out"] - oldest_count) / elapsed)) if elapsed else 0
        else:
            self._counts.clear()
        with self._lock:
            self._latest = summary
            self._history.append((now, summary.get("records_per_second", 0), summary.get("pending_records", 0)))
        return summary

    def snapshot(self):
        """The latest sample plus the history as [[epoch seconds, records/s, pending records], ...]."""
        self.start_sampling()
        with self._lock:
            return {**self._latest, "history": [list(row) for row in self._history]}

    def start_sampling(self, interval=1.0):
        """Sample once a second for as long as the process runs.  Started lazily by the first snapshot."""
        with self._lock:
            if self._sampler is not None:
                return
            self._sampler = threading.Thread(target=self._sample_forever, args=(interval,), daemon=True)
            self._sampler.start()

    def _sample_forever(self, interval):
        while True:
            started = time.monotonic()
            try:
                self.sample()
            except Exception:
                # One unexpected response must not stop the dashboard's history for good.
                traceback.print_exc()
            time.sleep(max(0, interval - (time.monotonic() - started)))

    def stop_with_savepoint(self, job_id, timeout=120):
        """Stop a job, saving its state (window counts and Kafka offsets).  Returns the savepoint's path."""
        trigger = self.fetch(f"/jobs/{job_id}/stop", body={"drain": False})["request-id"]
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = self.fetch(f"/jobs/{job_id}/savepoints/{trigger}")
            if result["status"]["id"] == "COMPLETED":
                operation = result["operation"]
                if "failure-cause" in operation:
                    raise RuntimeError(operation["failure-cause"].get("stack-trace", "Savepoint failed").splitlines()[0])
                return operation["location"]
            time.sleep(0.5)
        raise TimeoutError("Savepoint did not complete")
