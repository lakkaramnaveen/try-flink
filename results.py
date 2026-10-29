"""Reads the Flink job's output topic and keeps the latest per-minute counts for the dashboard's top videos."""

from __future__ import annotations

import json
import threading
import uuid

RESULTS_TOPIC = "clicks-per-minute"


class TopVideos:
    """The most recent minutes of per-video counts.

    Each (videoid, window_start) is set rather than added to, because the sink is at-least-once
    and can deliver a window's row twice.
    """

    def __init__(self, keep_windows=5):
        self._keep_windows = keep_windows
        self._windows: dict[str, dict[str, int]] = {}
        self._lock = threading.Lock()

    def add(self, row):
        with self._lock:
            self._windows.setdefault(row["window_start"], {})[row["videoid"]] = int(row["click_count"])
            # Window starts are "yyyy-MM-dd HH:mm:ss", so string order is time order.
            for old in sorted(self._windows)[:-self._keep_windows]:
                del self._windows[old]

    def clear(self):
        with self._lock:
            self._windows.clear()

    def latest(self, limit=10):
        with self._lock:
            if not self._windows:
                return {"window_start": None, "total": 0, "videos": []}
            window_start = max(self._windows)
            counts = self._windows[window_start]
            top = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]
            return {
                "window_start": window_start,
                "total": sum(counts.values()),
                "videos": [{"videoid": video, "click_count": count} for video, count in top],
            }


class ResultsConsumer:
    """Background Kafka consumer that feeds every result row into a TopVideos."""

    def __init__(self, top_videos, bootstrap_servers, consumer_factory=None):
        self._top_videos = top_videos
        self._bootstrap_servers = bootstrap_servers
        self._consumer_factory = consumer_factory
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        self.stop()
        self._top_videos.clear()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, args=(self._stop,), daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def _run(self, stop):
        factory = self._consumer_factory
        if factory is None:
            from confluent_kafka import Consumer as factory
        consumer = factory({
            "bootstrap.servers": self._bootstrap_servers,
            # A fresh group each time so the panel always reads the whole topic, and never commits.
            "group.id": f"control-panel-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            # Kafka being briefly unreachable is normal while Docker starts; don't log it.
            "error_cb": lambda _error: None,
            "log_level": 0,
        })
        consumer.subscribe([RESULTS_TOPIC])
        try:
            while not stop.is_set():
                message = consumer.poll(0.5)
                if message is None or message.error():
                    continue
                try:
                    self._top_videos.add(json.loads(message.value()))
                except (ValueError, KeyError, TypeError):
                    pass  # Not a result row.
        finally:
            consumer.close()
