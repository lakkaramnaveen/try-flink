"""Summarize bench/results.csv: the best and median throughput of each setup, fastest first.

usage: python3 bench/report.py [results.csv]
"""
import csv
import statistics
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else 'bench/results.csv'
try:
    with open(path, newline='') as results:
        rows = [row for row in csv.DictReader(results) if row['state'] == 'FINISHED']
except FileNotFoundError:
    sys.exit(f'No results yet: run `make bench` first ({path} is missing)')

# A setup is everything that affects throughput except the run's name and date.
runs = defaultdict(list)
for row in rows:
    setup = (int(row['parallelism']), row['query'], int(row['taskmanagers']), row['settings'])
    runs[setup].append(int(row['records_per_second']))

table = sorted(((max(rates), statistics.median(rates), len(rates), setup) for setup, rates in runs.items()), reverse=True)
header = f"{'best rec/s':>12} {'median':>12} {'runs':>5}  {'p':>2} {'query':<6} {'tm':>2}  settings"
print(header)
print('-' * len(header))
for best, median, count, (parallelism, query, taskmanagers, settings) in table:
    print(f'{best:>12,} {median:>12,.0f} {count:>5}  {parallelism:>2} {query:<6} {taskmanagers:>2}  {settings}')
