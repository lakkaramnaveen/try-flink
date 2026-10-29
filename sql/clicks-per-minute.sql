-- Counts clicks per video per minute and writes them to the clicks-per-minute topic.
-- Submit with `make sql`; read the results with `make results`.
SET 'pipeline.name' = 'clicks-per-minute';

-- Checkpoint every 10s.  On each checkpoint the source commits its Kafka offsets, so a resubmitted
-- job resumes where the last one stopped instead of re-reading the topic, and a failed task
-- recovers without losing window state.
SET 'execution.checkpointing.interval' = '10s';

-- Mini-batch was removed: it only applies to unbounded GROUP BY aggregates, not TUMBLE windows, and benchmarked no faster.
-- At slow sim speeds some of the 4 partitions can go quiet, which would hold back the watermark and stall every window.
-- Treat a partition with no data for 5s as idle so the others keep making progress.
SET 'table.exec.source.idle-timeout' = '5s';

CREATE TABLE youtube_inbound_stream (
    videoid STRING,
    `timestamp` STRING,
    click_time AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR click_time AS click_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'youtube-clicks',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-analysis-group',
    -- Resume from the offsets committed by the last checkpoint, or the start of the topic the first time.
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json'
);

CREATE TABLE clicks_per_minute (
    videoid STRING,
    window_start TIMESTAMP(3),
    click_count BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks-per-minute',
    'properties.bootstrap.servers' = 'kafka:9092',
    -- A window can be written twice if the job restarts between a write and the next checkpoint.
    -- Readers keep the latest count per (videoid, window_start), so a repeat is harmless.
    'sink.delivery-guarantee' = 'at-least-once',
    'format' = 'json'
);

INSERT INTO clicks_per_minute
SELECT
    videoid,
    TUMBLE_START(click_time, INTERVAL '1' MINUTE) AS window_start,
    COUNT(*) AS click_count
FROM youtube_inbound_stream
GROUP BY
    videoid,
    TUMBLE(click_time, INTERVAL '1' MINUTE);
