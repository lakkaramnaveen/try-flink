"""Producer migration regressions with private files and no Kafka connection."""

from collections import deque
import csv
import io
import json
import os
from pathlib import Path
import runpy
import tempfile
import threading
import types
import unittest
from unittest.mock import Mock, patch

from traffic_metrics import HEADER, RECORD


SCRIPT = Path(__file__).resolve().with_name('run-sim.py')
ROWS = (
    ('video-1', '2025-01-01 00:00:00'),
    ('video-2', '2025-01-01 00:00:01'),
)


class FakeProducer:
    def __init__(self, errors=(), queue_full_once=False, deliver_on_poll=False, delivery_gate=None):
        self.errors = deque(errors)
        self.queue_full_once = queue_full_once
        self.deliver_on_poll = deliver_on_poll
        self.delivery_gate = delivery_gate
        self.condition = threading.Condition()
        self.pending = deque()
        self.produce_calls = []
        self.accepted = []
        self.delivered = threading.Event()
        self.flush_count = 0
        self.pending_at_flush = None
        self.metrics_stopped_at_flush = None

    def produce(self, topic, value, on_delivery):
        with self.condition:
            self.produce_calls.append((topic, value))
            if self.queue_full_once:
                self.queue_full_once = False
                raise BufferError('Local: Queue full')
            self.accepted.append((topic, value))
            error = self.errors.popleft() if self.errors else None
            self.pending.append((on_delivery, error))
            self.condition.notify_all()

    def _deliver(self, callbacks):
        for callback, error in callbacks:
            callback(error, object())
            self.delivered.set()

    def poll(self, timeout):
        with self.condition:
            if not self.pending or not self.deliver_on_poll or (
                    self.delivery_gate is not None and not self.delivery_gate.is_set()):
                self.condition.wait(timeout)
            can_deliver = self.deliver_on_poll and (
                self.delivery_gate is None or self.delivery_gate.is_set())
            callbacks = list(self.pending) if can_deliver else []
            if can_deliver:
                self.pending.clear()
        self._deliver(callbacks)
        return len(callbacks)

    def flush(self):
        self.flush_count += 1
        with self.condition:
            callbacks = list(self.pending)
            self.pending.clear()
        self.pending_at_flush = len(callbacks)
        if callbacks:
            self.metrics_stopped_at_flush = callbacks[0][0].__globals__['metrics_stop'].is_set()
        self._deliver(callbacks)
        return 0


class ImmediateClock:
    def __init__(self, read_control):
        self.initial_control = read_control()

    def wait_until(self, seconds_since_start):
        pass


class SimulatorProducerTests(unittest.TestCase):
    def run_simulator(self, producer, clock=ImmediateClock, settings=None):
        """Execute the actual script, replacing only its external boundaries."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (root / 'youtube-clicks.csv').open('w', newline='') as source:
                writer = csv.writer(source)
                writer.writerow(('videoid', 'timestamp'))
                writer.writerows(ROWS)
            control = root / 'control.json'
            control.write_text(json.dumps({'paused': False, 'speed': 10000, **(settings or {})}))
            metrics_path = root / 'metrics.bin'
            producer_factory = Mock(return_value=producer)
            kafka = types.ModuleType('confluent_kafka')
            kafka.Producer = producer_factory
            previous_directory = Path.cwd()
            try:
                os.chdir(root)
                with patch.dict('sys.modules', {'confluent_kafka': kafka}), \
                        patch.dict(os.environ, {
                            'SIM_CONTROL_FILE': str(control),
                            'SIM_METRICS_FILE': str(metrics_path),
                            'SIM_SPEED': '120',
                        }), \
                        patch('simulation_clock.SimulationClock', clock), \
                        patch('sys.stdout', new_callable=io.StringIO):
                    namespace = runpy.run_path(str(SCRIPT))
            finally:
                os.chdir(previous_directory)

            # Include any periodic samples if a slow test machine crossed a tick.
            totals = {name: namespace['metrics'][name] for name in ('attempted', 'acked', 'errors')}
            contents = metrics_path.read_bytes()
            self.assertEqual(contents[:len(HEADER)], HEADER)
            for offset in range(len(HEADER), len(contents), RECORD.size):
                _, _, attempted, acked, errors = RECORD.unpack(contents[offset:offset + RECORD.size])
                for name, value in zip(totals, (attempted, acked, errors)):
                    totals[name] += value
            self.assertFalse(namespace['metrics_writer'].is_alive())
            return namespace, totals, producer_factory

    def test_wire_payload_and_final_delivery_metrics_are_preserved(self):
        producer = FakeProducer()
        _, totals, _ = self.run_simulator(producer)

        expected = [
            ('youtube-clicks', json.dumps({'videoid': video, 'timestamp': timestamp}).encode('utf-8'))
            for video, timestamp in ROWS
        ]
        self.assertEqual(producer.accepted, expected)
        self.assertEqual(totals, {'attempted': 2, 'acked': 2, 'errors': 0})
        self.assertEqual(producer.flush_count, 1)
        self.assertEqual(producer.pending_at_flush, 2)
        self.assertFalse(producer.metrics_stopped_at_flush)
        self.assertFalse(producer.pending)

    def test_full_queue_retries_without_dropping_or_double_counting(self):
        producer = FakeProducer(queue_full_once=True)
        _, totals, _ = self.run_simulator(producer)

        self.assertEqual(len(producer.produce_calls), 3)
        self.assertEqual(producer.produce_calls[0], producer.produce_calls[1])
        self.assertEqual(len(producer.accepted), 2)
        self.assertEqual(totals, {'attempted': 2, 'acked': 2, 'errors': 0})

    def test_failed_delivery_is_counted_separately_from_acknowledgements(self):
        producer = FakeProducer(errors=(None, RuntimeError('Broker rejected record')))
        _, totals, _ = self.run_simulator(producer)

        self.assertEqual(totals, {'attempted': 2, 'acked': 1, 'errors': 1})
        self.assertFalse(producer.pending)

    def test_delivery_callbacks_progress_while_clock_waits(self):
        waiting = threading.Event()
        producer = FakeProducer(deliver_on_poll=True, delivery_gate=waiting)

        class WaitingClock(ImmediateClock):
            callback_during_wait = False

            def wait_until(self, seconds_since_start):
                if seconds_since_start > 0:
                    # Reports become available only after replay enters a gap.
                    waiting.set()
                    with producer.condition:
                        producer.condition.notify_all()
                    self.callback_during_wait = producer.delivered.wait(timeout=1)

        namespace, totals, _ = self.run_simulator(producer, WaitingClock)

        self.assertTrue(namespace['simulation_clock'].callback_during_wait)
        self.assertEqual(totals, {'attempted': 2, 'acked': 2, 'errors': 0})
        self.assertFalse(namespace['delivery_poller'].is_alive())

    def test_burst_sends_every_click_burst_factor_times(self):
        producer = FakeProducer()
        _, totals, _ = self.run_simulator(producer, settings={'burst_until': 4e9, 'burst_factor': 3})

        self.assertEqual([value for _, value in producer.accepted], [
            json.dumps({'videoid': video, 'timestamp': timestamp}).encode('utf-8')
            for video, timestamp in ROWS for _ in range(3)
        ])
        self.assertEqual(totals, {'attempted': 6, 'acked': 6, 'errors': 0})

    def test_expired_burst_is_ignored(self):
        producer = FakeProducer()
        self.run_simulator(producer, settings={'burst_until': 1, 'burst_factor': 3})
        self.assertEqual(len(producer.accepted), 2)

    def test_late_clicks_keep_their_timestamp_and_are_sent_300_simulated_seconds_later(self):
        producer = FakeProducer()
        waits = []

        class RecordingClock(ImmediateClock):
            def wait_until(self, seconds_since_start):
                waits.append((seconds_since_start, len(producer.accepted)))

        _, totals, _ = self.run_simulator(producer, RecordingClock, settings={'late_percent': 100})

        self.assertEqual([json.loads(value)['timestamp'] for _, value in producer.accepted], [ts for _, ts in ROWS])
        # Both rows are read (at 0s and 1s) before anything is sent, then released at 300s and 301s.
        self.assertEqual(waits, [(0, 0), (1, 0), (300, 0), (301, 1)])
        self.assertEqual(totals, {'attempted': 2, 'acked': 2, 'errors': 0})


if __name__ == '__main__':
    unittest.main()
