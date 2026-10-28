"""Blast N copies of youtube-clicks.csv (each shifted one day later) into a topic as fast as possible.

usage: python3 bench/bulk-load.py <topic> <days>
"""
import csv, json, sys
from datetime import datetime, timedelta
from confluent_kafka import Producer
topic, days = sys.argv[1], int(sys.argv[2])
p = Producer({'bootstrap.servers': '127.0.0.1:19092', 'linger.ms': 20, 'batch.size': 1_000_000,
              'compression.type': 'lz4', 'queue.buffering.max.messages': 1_000_000, 'acks': 1})
rows = list(csv.reader(open('youtube-clicks.csv')))[1:]
n = 0
for d in range(days):
    for vid, ts in rows:
        t = (datetime.fromisoformat(ts) + timedelta(days=d)).strftime('%Y-%m-%d %H:%M:%S')
        while True:
            try: p.produce(topic, json.dumps({"videoid": vid, "timestamp": t}).encode()); break
            except BufferError: p.poll(0.05)
        n += 1
    p.poll(0)
p.flush(); print("produced", n)
