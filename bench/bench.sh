#!/bin/bash
# Measures Flink's max throughput by draining the bench-clicks backlog into a blackhole sink.
# usage: bench/bench.sh <name> <parallelism> <legacy|tvf> ["SET 'key' = 'value';" ...]
# e.g.   bench/bench.sh p4 4 legacy "SET 'pipeline.object-reuse' = 'true';"
set -euo pipefail
cd "$(dirname "$0")/.."
NAME=$1 P=$2 Q=$3; shift 3

# The total backlog size, summed across partitions.
TOTAL=$(docker compose exec -T kafka rpk topic describe bench-clicks -p | awk 'NR>1{s+=$NF}END{print s}')

{
echo "SET 'pipeline.name' = 'bench-$NAME';"
echo "SET 'table.dml-sync' = 'true';"  # Block until the bounded job finishes.
echo "SET 'parallelism.default' = '$P';"
for s in "$@"; do echo "$s"; done
cat <<SQL
CREATE TABLE src (
    videoid STRING,
    \`timestamp\` STRING,
    click_time AS TO_TIMESTAMP(\`timestamp\`, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR click_time AS click_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bench-clicks',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'bench-$NAME-$RANDOM',
    'scan.startup.mode' = 'earliest-offset',
    'scan.bounded.mode' = 'latest-offset',
    'format' = 'json'
);
CREATE TABLE sink (videoid STRING, window_start TIMESTAMP(3), click_count BIGINT) WITH ('connector' = 'blackhole');
SQL
if [ "$Q" = legacy ]; then
    echo "INSERT INTO sink SELECT videoid, TUMBLE_START(click_time, INTERVAL '1' MINUTE), COUNT(*) FROM src GROUP BY videoid, TUMBLE(click_time, INTERVAL '1' MINUTE);"
else
    echo "INSERT INTO sink SELECT videoid, window_start, COUNT(*) FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(click_time), INTERVAL '1' MINUTE)) GROUP BY videoid, window_start, window_end;"
fi
} > sql/_bench.sql

docker compose exec -T jobmanager ./bin/sql-client.sh -f /opt/flink/sql/_bench.sql > /dev/null
curl -s localhost:8081/jobs/overview | python3 -c "
import json, sys
job = max((j for j in json.load(sys.stdin)['jobs'] if j['name'] == 'bench-$NAME'), key=lambda j: j['start-time'])
seconds = job['duration'] / 1000
print(f\"$NAME: {job['state']} p=$P q=$Q  {seconds:.1f}s  {$TOTAL / seconds:,.0f} rec/s\")"
