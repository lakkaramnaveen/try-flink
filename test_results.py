"""Top videos bookkeeping and the results consumer loop, with a fake Kafka consumer."""

import json
import threading
import unittest

from results import ResultsConsumer, TopVideos


def row(video, window, count):
    return {"videoid": video, "window_start": window, "click_count": count}


class TopVideosTests(unittest.TestCase):
    def test_latest_window_ranked_by_count_then_name(self):
        top = TopVideos()
        for values in (("a", "12:00", 9), ("b", "12:01", 3), ("c", "12:01", 5), ("a", "12:01", 3)):
            top.add(row(*values))
        self.assertEqual(top.latest(limit=2), {
            "window_start": "12:01", "total": 11,
            "videos": [{"videoid": "c", "click_count": 5}, {"videoid": "a", "click_count": 3}],
        })

    def test_a_repeated_row_replaces_rather_than_adds(self):
        top = TopVideos()
        top.add(row("a", "12:00", 4))
        top.add(row("a", "12:00", 4))
        self.assertEqual(top.latest()["total"], 4)

    def test_only_recent_windows_are_kept(self):
        top = TopVideos(keep_windows=2)
        for minute in ("12:00", "12:01", "12:02"):
            top.add(row("a", minute, 1))
        self.assertEqual(sorted(top._windows), ["12:01", "12:02"])

    def test_empty(self):
        self.assertEqual(TopVideos().latest(), {"window_start": None, "total": 0, "videos": []})


class FakeMessage:
    def __init__(self, value, error=None):
        self._value, self._error = value, error

    def value(self):
        return self._value

    def error(self):
        return self._error


class FakeConsumer:
    def __init__(self, messages):
        self.messages = list(messages)
        self.config = None
        self.topics = None
        self.closed = threading.Event()
        self.drained = threading.Event()

    def __call__(self, config):
        self.config = config
        return self

    def subscribe(self, topics):
        self.topics = topics

    def poll(self, timeout):
        if self.messages:
            return self.messages.pop(0)
        self.drained.set()
        threading.Event().wait(0.01)
        return None

    def close(self):
        self.closed.set()


class ResultsConsumerTests(unittest.TestCase):
    def test_rows_are_collected_and_bad_messages_skipped(self):
        fake = FakeConsumer([
            FakeMessage(json.dumps(row("a", "12:00", 2)).encode()),
            FakeMessage(None, error="partition EOF"),
            FakeMessage(b"not json"),
            FakeMessage(json.dumps({"unexpected": 1}).encode()),
            FakeMessage(json.dumps(row("b", "12:00", 5)).encode()),
        ])
        top = TopVideos()
        top.add(row("stale", "11:00", 1))
        consumer = ResultsConsumer(top, "kafka:9092", consumer_factory=fake)
        consumer.start()
        self.assertTrue(fake.drained.wait(2))
        consumer.stop()

        self.assertTrue(fake.closed.is_set())
        self.assertEqual(fake.topics, ["clicks-per-minute"])
        self.assertEqual(fake.config["auto.offset.reset"], "earliest")
        self.assertFalse(fake.config["enable.auto.commit"])
        self.assertEqual([video["videoid"] for video in top.latest()["videos"]], ["b", "a"])  # Start cleared "stale".


if __name__ == "__main__":
    unittest.main()
