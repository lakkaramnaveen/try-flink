# Architecture

## Data flow

```mermaid
flowchart LR
    csv[youtube-clicks.csv<br/>100k clicks, one day] --> sim[run-sim.py<br/>replays at N× speed]
    sim -- JSON clicks --> kafka[(Redpanda / Kafka<br/>topic youtube-clicks<br/>4 partitions)]
    kafka --> flink[Flink SQL job<br/>clicks per video per minute<br/>parallelism 4]
    flink --> results[.flink-sql.log<br/>or the sql-client terminal]

    panel[app.py<br/>control panel :5050] -. starts/stops .-> sim
    panel -. docker compose up/down .-> kafka
    panel -. submits SQL .-> flink
    sim -. .traffic-metrics.bin .-> panel
    panel -. REST :8081 .-> flink
```

Everything runs on one laptop.  Kafka and Flink are in Docker (`docker-compose.yml`); the simulator and the control panel run on the host.

| Service | Port | Purpose |
| --- | --- | --- |
| Control panel | 5050 | Start/stop/pause, speed, traffic chart, Flink job summary |
| Flink jobmanager | 8081 | Flink web UI and REST API |
| Redpanda (Kafka API) | 19092 from the host, `kafka:9092` inside Docker | Click events |
| Redpanda Console | 8080 | Browse topics and messages |

## The simulator

`run-sim.py` reads the CSV in timestamp order and produces each click to Kafka when the simulated clock reaches it.  `SimulationClock` turns wall time into simulated time at the current speed and can be paused or sped up mid-run.  It re-reads `.simulation-control.json` at most every 200ms, which is how the control panel controls a separate process without an IPC channel.

Once a second it appends a 24-byte sample (expected, attempted, acknowledged, and failed sends) to `.traffic-metrics.bin`.  The format lives in `traffic_metrics.py` and `static/traffic-metrics.mjs`, and a shared fixture (`test-fixtures/traffic-metrics.json`) keeps the two in agreement.  A fixed-size binary record lets the panel read only the last 15 minutes by seeking, however long the run.

## The Flink job

`sql/clicks-per-minute.sql` reads JSON from `youtube-clicks`, derives `click_time` from the event's own timestamp, and counts clicks per video in one-minute tumbling windows.

 - **Event time, not arrival time.**  Windows follow the simulated clock, so the counts are the same at 1× or 10000×.
 - **Watermark: 10 seconds.**  A minute is emitted once a click 10 seconds past its end has been seen.  Later clicks for that minute are dropped.
 - **Idle partitions: 5 seconds.**  The watermark is the minimum across partitions, so one quiet partition would stall every window.  After 5s without data a partition is ignored.
 - **The last window never closes** when traffic stops, because nothing advances the watermark past it.  That's expected for an unbounded stream.

## Tuning decisions

Measured with `bench/` (see the README for the numbers):

 - **Parallelism 4 = partition count.**  This gives 2.5× the throughput of parallelism 1.  Each Kafka partition is read by at most one source task, so parallelism above the partition count only helps the aggregation, which isn't the bottleneck here.  To scale further, add partitions and parallelism together.
 - **8 task slots.**  Each slot gets an equal share of the TaskManager's memory.  The original 100 slots left each about 5MB of managed memory.
 - **No mini-batch.**  It only affects unbounded `GROUP BY` aggregates, not window aggregates, and measured no difference.
 - **Legacy `GROUP BY TUMBLE(...)` over window TVFs.**  The TVF syntax was ~10% slower for this query.  TVFs are the recommended syntax going forward and support more window types, so revisit this if the query grows.

## The control panel

`app.py` is a small Flask app.  Starting runs in a background thread: `docker compose up`, wait for Flink's REST API, then launch the SQL client and the simulator.  This keeps `/api/status` responsive during the minute Docker can take.  A stop during start-up bumps a generation counter, and the boot thread checks it before each step and gives up.

Stopping the panel (Ctrl+C or SIGTERM) runs `docker compose down`.  The hook is registered only when `app.py` is run directly, so tests can import it safely.

`/api/flink` summarizes the newest running job from Flink's REST API:

 - **Records/s:** the sum of `numRecordsOutPerSecond` across the source subtasks.
 - **Busy and backpressure:** the worst subtask across all operators.  A task near 100% busy is the bottleneck.  Backpressure on a task means the task after it is the bottleneck.

## Known limitations and next steps

 - **No checkpointing.**  Kafka offsets aren't committed, and a failed job restarts from the earliest offset.  Enabling `execution.checkpointing.interval` fixes both.  The catch: the SQL client's result view then only shows rows after each checkpoint completes, so it should come with a real sink (e.g. a `clicks-per-minute` Kafka topic) instead of printing.
 - **Results only go to a log.**  Writing them to a Kafka topic would let the panel chart the top videos.
 - **One TaskManager.**  To try scaling out, run `docker compose up -d --scale taskmanager=2`.
 - **Kafka data is not persisted.**  `docker compose down` deletes it, which keeps runs reproducible.
