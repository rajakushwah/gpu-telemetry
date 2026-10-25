-- Idempotent schema; collectors and API instances may run it concurrently (guarded by an advisory lock).
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS gpus (
  uuid       TEXT PRIMARY KEY,
  gpu_id     TEXT NOT NULL,
  device     TEXT NOT NULL DEFAULT '',
  model_name TEXT NOT NULL DEFAULT '',
  hostname   TEXT NOT NULL,
  last_seen  TIMESTAMPTZ NOT NULL
);

-- The unique key deduplicates at-least-once redeliveries and serves the per-GPU time-ordered query.
CREATE TABLE IF NOT EXISTS telemetry (
  ts       TIMESTAMPTZ      NOT NULL,
  gpu_uuid TEXT             NOT NULL,
  metric   TEXT             NOT NULL,
  value    DOUBLE PRECISION NOT NULL,
  labels   JSONB,
  UNIQUE (gpu_uuid, ts, metric)
);

SELECT create_hypertable('telemetry', 'ts', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
SELECT add_retention_policy('telemetry', INTERVAL '7 days', if_not_exists => TRUE);

-- Columnar compression for chunks older than 2 hours; segmenting by GPU keeps per-GPU queries fast.
-- Guarded because compression settings can't be changed once chunks are compressed.
DO $$ BEGIN
  IF NOT (SELECT compression_enabled FROM timescaledb_information.hypertables WHERE hypertable_name = 'telemetry') THEN
    ALTER TABLE telemetry SET (timescaledb.compress, timescaledb.compress_segmentby = 'gpu_uuid', timescaledb.compress_orderby = 'ts, metric');
  END IF;
END $$;
SELECT add_compression_policy('telemetry', INTERVAL '2 hours', if_not_exists => TRUE);
