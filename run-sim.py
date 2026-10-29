import csv
import heapq
import json
import os
import random
import threading
import time
from collections import Counter
from datetime import datetime, timedelta
from itertools import chain, count
from confluent_kafka import Producer

from simulation_clock import SimulationClock
from traffic_metrics import HEADER, pack_sample

"""
Design:
The point of this script is to simulate the traffic at a rate that mimicks production load and timing.

The fake event data is already generated with timestamps, so this code simply forwards it at a rate to correspond with those timestamps.

It is scaled down though because we're replaying a full 24 hours of traffic in just a few minutes.  Additionally, the servers are running locally in docker on a single laptop, so the traffic is scaled to match.
"""

# Connect to our local Kafka broker
producer = Producer({
    'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:19092'),
    'linger.ms': 5,  # Allow short batches to reduce Kafka request overhead.
    'batch.size': 16384,
    'acks': 1,
    'retries': 0,
    'delivery.timeout.ms': 30000,
    'queue.buffering.max.messages': 100_000,  # This is the default.  Making it explicit for ease of tuning.
    'queue.buffering.max.kbytes': 1_048_576,  # 1 GiB. This is the default.  Making it explicit for ease of tuning.
})

TOPIC = 'youtube-clicks'
CSV_FILE = 'youtube-clicks.csv'
CONTROL_FILE = os.environ.get('SIM_CONTROL_FILE', '.simulation-control.json')
METRICS_FILE = os.environ.get('SIM_METRICS_FILE', '.traffic-metrics.bin')
DEFAULT_SPEED = float(os.environ.get('SIM_SPEED', '120'))
CONTROL_POLL_INTERVAL = 0.2
# Late clicks are held back this many simulated seconds, well past the 60s window plus the 10s watermark delay.
# Flink only advances the watermark every 200ms of wall time, so at high speeds it trails by many simulated
# seconds and some "late" clicks still land in open windows.  Measured: at 120x every late click is dropped,
# at 1000x about half are.
LATE_DELAY_SECONDS = 300
DEFAULT_SETTINGS = {'paused': False, 'speed': DEFAULT_SPEED, 'late_percent': 0, 'burst_until': 0, 'burst_factor': 1}
cached_settings = DEFAULT_SETTINGS
next_control_read = 0.0

metrics_lock = threading.Lock()
metrics = {'expected': 0, 'attempted': 0, 'acked': 0, 'errors': 0}
metrics_stop = threading.Event()
delivery_stop = threading.Event()


def record_metric(name):
    with metrics_lock:
        metrics[name] += 1


def set_expected_rate(value):
    with metrics_lock:
        metrics['expected'] = value


def on_delivery(error, _message):
    record_metric('errors' if error is not None else 'acked')


def poll_deliveries():
    """Keep acknowledgements flowing even while simulation time is paused."""
    while not delivery_stop.is_set():
        producer.poll(0.1)


def write_metrics():
    """Write one aggregate sample per second so the dashboard stays cheap."""
    with open(METRICS_FILE, 'wb') as metrics_file:
        metrics_file.write(HEADER)
        metrics_file.flush()
        while not metrics_stop.wait(1):
            with metrics_lock:
                sample = dict(metrics)
                metrics.update(attempted=0, acked=0, errors=0)
            metrics_file.write(pack_sample(sample, time.time()))
            metrics_file.flush()


def read_settings():
    """Return the control panel's settings, reading the file at most every 200 ms.

    paused, speed: drive the simulation clock.
    late_percent: the share of clicks to deliver LATE_DELAY_SECONDS late.
    burst_until, burst_factor: until this epoch time, send every click burst_factor times (a viral spike).
    """
    global cached_settings, next_control_read
    now = time.monotonic()
    if now < next_control_read:
        return cached_settings
    try:
        with open(CONTROL_FILE, 'r') as control_file:
            value = json.load(control_file)
        cached_settings = {
            'paused': bool(value.get('paused', False)),
            'speed': max(float(value.get('speed', DEFAULT_SPEED)), 0.01),
            'late_percent': float(value.get('late_percent', 0)),
            'burst_until': float(value.get('burst_until', 0)),
            'burst_factor': max(int(value.get('burst_factor', 1)), 1),
        }
    except (FileNotFoundError, json.JSONDecodeError, TypeError, ValueError, AttributeError):
        cached_settings = DEFAULT_SETTINGS
    next_control_read = time.monotonic() + CONTROL_POLL_INTERVAL
    return cached_settings


def read_control():
    """Return (paused, speed) for the simulation clock."""
    settings = read_settings()
    return settings['paused'], settings['speed']

print("Starting throttled real-time stream simulation...", flush=True)
metrics_writer = threading.Thread(target=write_metrics, daemon=True)
metrics_writer.start()
delivery_poller = threading.Thread(target=poll_deliveries, daemon=True)
delivery_poller.start()

with open(CSV_FILE, 'r') as source_file:
    source_reader = csv.reader(source_file)
    next(source_reader)
    expected_events_per_second = Counter(row[1] for row in source_reader)

expected_start = min(datetime.fromisoformat(value) for value in expected_events_per_second)
expected_end = max(datetime.fromisoformat(value) for value in expected_events_per_second)
expected_prefix = [0]
for offset in range(int((expected_end - expected_start).total_seconds()) + 1):
    bucket = (expected_start + timedelta(seconds=offset)).strftime("%Y-%m-%d %H:%M:%S")
    expected_prefix.append(expected_prefix[-1] + expected_events_per_second.get(bucket, 0))


def expected_rate(virtual_time, speed):
    """Count events in the last one-real-second window of simulated time."""
    window_seconds = max(1, int(round(speed)))
    current_offset = int((virtual_time - expected_start).total_seconds())
    window_start = max(0, current_offset - window_seconds + 1)
    return expected_prefix[current_offset + 1] - expected_prefix[window_start]

def produce(payload):
    while True:
        try:
            producer.produce(TOPIC, value=payload, on_delivery=on_delivery)
            break
        except BufferError:
            # Wait for queue space, then retry this event without dropping it.
            producer.poll(0.1)
    record_metric('attempted')


# Late clicks waiting to be sent, as a heap of (simulated send time, tiebreaker, payload).
late_clicks = []
late_sequence = count()


def release_late_clicks(until_seconds):
    """Send every held-back click due at or before this simulated time."""
    while late_clicks and late_clicks[0][0] <= until_seconds:
        send_at, _, payload = heapq.heappop(late_clicks)
        simulation_clock.wait_until(send_at)
        produce(payload)


with open(CSV_FILE, 'r') as f:
    reader = csv.reader(f)
    header = next(reader) # Skip header
    
    first_row = next(reader)
    first_virtual_time = datetime.fromisoformat(first_row[1])
    simulation_clock = SimulationClock(read_control)
    
    for row in chain((first_row,), reader):
        vid, ts_str = row[0], row[1]
        current_virtual_time = datetime.fromisoformat(ts_str)
        seconds_since_start = (current_virtual_time - first_virtual_time).total_seconds()
        release_late_clicks(seconds_since_start)
        simulation_clock.wait_until(seconds_since_start)

        # Emit the event to Kafka, keeping its original timestamp even if it's sent late.
        payload = json.dumps({"videoid": vid, "timestamp": ts_str}).encode('utf-8')
        settings = read_settings()
        copies = settings['burst_factor'] if time.time() < settings['burst_until'] else 1
        if random.random() * 100 < settings['late_percent']:
            heapq.heappush(late_clicks, (seconds_since_start + LATE_DELAY_SECONDS, next(late_sequence), payload))
        else:
            for _ in range(copies):
                produce(payload)
        set_expected_rate(expected_rate(current_virtual_time, settings['speed']) * copies)

    release_late_clicks(float('inf'))

delivery_stop.set()
delivery_poller.join(timeout=2)
producer.flush()
metrics_stop.set()
metrics_writer.join(timeout=2)
print("Stream simulation complete.", flush=True)

