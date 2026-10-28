# Goal
Sep 23, 2026:  Will finally try out Flink.  Will simulate some load and tune a Flink instance.

A simulator replays a day of fake YouTube clicks into Kafka, and a Flink SQL job counts clicks per video per minute.  A small control panel starts everything and charts the traffic.  How the pieces fit together is in [`docs/architecture.md`](docs/architecture.md).

# Setup
1. Install docker ([download the .dmg from the website](https://www.docker.com/products/docker-desktop/) because homebrew has dropped support for intel mac).
2. `make install` (or `python3 -m pip install -r requirements.txt`).  This installs Flask, the Kafka client, and numpy.

Docker will handle downloading everything else.  The first start builds a small Flink image with the Kafka connector baked in.

Run `make` to list every shortcut.

# How to run it via the control panel

Start Docker Desktop, then start the flask app:

```sh
make panel   # or: python3 app.py
```

Then open http://localhost:5050, and click the play button.  This will start the docker containers, submit the Flink query and start the traffic.

 - The speed picker sets how fast simulated time runs: at 120× the day of traffic takes 12 minutes, at 10000× about 9 seconds.
 - The line under the status shows the running Flink job: its parallelism, how many records per second it is consuming, its busiest task, and backpressure.  Anything highlighted red is worth tuning.
 - The query's results are written to `.flink-sql.log`.  Follow them with `make results`.
 - Stopping the panel (Ctrl+C) stops the docker containers too.

The panel uses port 5050 because macOS's AirPlay Receiver already listens on 5000.  Set `PORT` to use another one.

# How to run it manually

## 1. Start the flink and kafka inside of docker
Start the docker desktop app then run `make up` (`docker compose up -d --build`).
<br>
<br>Clean restart: `make restart`.  Kafka has no volume, so this also deletes all the clicks.

## 2. Generate the fake data
`youtube-clicks.csv` is already checked in.  To regenerate it run `make data`.  The generator is seeded, so it produces the same file every time unless you change `SEED`.

## 3. Send the fake data to the kafka
Run `python3 run-sim.py`.  Set `SIM_SPEED` (default 120) to change the speed, or edit `.simulation-control.json` while it runs.

## 4. Get flink to consume the data from Kafka

The query lives in [`sql/clicks-per-minute.sql`](sql/clicks-per-minute.sql) which is mirrored into the docker container.  Run it like this:

```sh
make sql
```

The output is clicks **per video per simulated minute**.  A minute is printed once the watermark passes it, i.e. once clicks from 10 seconds into the next minute have arrived.

For an interactive SQL prompt instead:

```sh
docker compose exec jobmanager ./bin/sql-client.sh
```

# How to monitor

Run this command to watch the events go through (they will each print in the terminal):
`docker compose exec kafka rpk topic consume youtube-clicks`

View Flink's web dashboard: http://localhost:8081

View Kafka's web dashboard: http://localhost:8080/overview

The traffic chart and the Flink job summary are in the control panel: http://localhost:5050.

# How to benchmark / tune Flink
The simulated day is only 100k clicks, so it can't stress Flink.  Instead, load a big backlog and time how fast Flink drains it:

```sh
make up
make bench-data   # 20M events into the bench-clicks topic.  BENCH_DAYS=50 for fewer.
make bench        # parallelism 1 vs 4
```

Or run `bench/bench.sh <name> <parallelism> <legacy|tvf> ["SET 'key' = 'value';" ...]` to try other settings.  Each run reads the whole backlog with a fresh consumer group, writes to a blackhole sink so printing isn't the bottleneck, and reports records per second.

Results on a 14 core laptop, 20M events:

| Setup | Throughput |
| --- | --- |
| parallelism 1, TUMBLE group window, mini-batch on | ~600k rec/s |
| parallelism 1, window TVF | ~500k rec/s |
| parallelism 4, TUMBLE group window, mini-batch on or off | ~1.5M rec/s |
| parallelism 4, window TVF | ~1.3M rec/s (~1.5M with `pipeline.object-reuse`) |
| parallelism 8 (topic only has 4 partitions) | ~1.6M rec/s |

Takeaways: parallelism matching the partition count is the big win; mini-batch doesn't affect window aggregates; going past the partition count barely helps because the Kafka source is capped at one reader per partition.

# How to run the tests
`make test` runs everything.  None of the tests need Docker or Kafka.
 - `python3 -m unittest test_app test_traffic_metrics test_simulation_clock test_simulator_producer`
 - `node test_traffic_chart.cjs`

They also run on every pull request (`.github/workflows/test.yml`).

# Files
 - `app.py` - The control panel's Flask server: starts and stops the stack and serves the dashboard and its API.
 - `control-panel.html`, `static/` - The dashboard page, its styles and scripts.  `static/traffic-metrics.mjs` decodes the traffic samples.
 - `run-sim.py` - This sends the clicks to the kafka, respecting the timestamps for each event.
 - `simulation_clock.py` - The simulator's clock, which can be paused and sped up while running.
 - `traffic_metrics.py` - The binary format the simulator writes its once-a-second traffic samples in, for the chart.
 - `generate-fake-clicks-data.py` - This creates `youtube-clicks.csv` with the columns `videoid` and `timestamp`.
 - `youtube-clicks.csv` - This contains the generated fake click data.
 - `sql/clicks-per-minute.sql` - This contains the clicks-per-minute query in flinkSQL.
 - `docker-compose.yml` - This describes and configures a kafka and a flink instance.
 - `flink/Dockerfile` - The Flink image, with the Kafka connector baked in.
 - `bench/` - The throughput benchmark: `bulk-load.py` fills a topic as fast as possible, `bench.sh` times Flink draining it.
 - `docs/architecture.md` - How the pieces fit together, and the design decisions.

# Debugging
 - Open docker desktop and confirm that all containers are green.  If not, review the logs in docker (or `make logs`), fix it, then restart docker.
 - The control panel says "Could not start Docker services": Docker Desktop isn't running.
 - No query results: check `.flink-sql.log` for SQL errors, and the job's status at http://localhost:8081.

Network related commands:
 - Confirm that the right ports are open with `nc`.  e.g. `nc -zv 127.0.0.1 19092` (Kafka), `nc -zv 127.0.0.1 8081` (Flink)
 - Open a command line inside of the docker network to run `nc` inside as well: `docker run --rm -it --network try-flink_default alpine sh`
   - `alpine` in a kind of linux like `ubuntu` and `try-flink_default` is the name of the local network running inside of docker.  Inside it, Kafka is at `kafka:9092`.
 - Run `docker network ls` to list the docker internal networks.
