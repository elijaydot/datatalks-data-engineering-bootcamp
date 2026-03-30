-- ─────────────────────────────────────────────
-- KEYSPACE
-- ─────────────────────────────────────────────
CREATE KEYSPACE IF NOT EXISTS crypto
    WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
    };

USE crypto;

-- ─────────────────────────────────────────────
-- TABLE 1: Raw price ticks (every tick from Flink)
-- Partition key : symbol  → all BTC rows on same node
-- Clustering    : timestamp DESC → latest first
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS price_ticks (
    symbol      TEXT,
    timestamp   TIMESTAMP,
    price       DOUBLE,
    volume      DOUBLE,
    exchange    TEXT,
    bid         DOUBLE,
    ask         DOUBLE,
    PRIMARY KEY ((symbol), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND default_time_to_live = 604800;
-- TTL = 7 days. Old ticks auto-expire, keeping table lean.
-- Historical data lives in DuckDB instead.

-- ─────────────────────────────────────────────
-- TABLE 2: 1-minute OHLCV candles (aggregated by Flink)
-- Partition key : symbol
-- Clustering    : window_start DESC → latest candle first
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ohlcv_1min (
    symbol          TEXT,
    window_start    TIMESTAMP,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          DOUBLE,
    tick_count      INT,
    PRIMARY KEY ((symbol), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 2592000;
-- TTL = 30 days of candles in Cassandra.
-- Older history is in DuckDB.

-- ─────────────────────────────────────────────
-- TABLE 3: Anomalies (flagged by Flink)
-- Partition key : symbol
-- Clustering    : timestamp DESC
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS anomalies (
    symbol          TEXT,
    timestamp       TIMESTAMP,
    price           DOUBLE,
    volume          DOUBLE,
    anomaly_type    TEXT,
    anomaly_detail  TEXT,
    PRIMARY KEY ((symbol), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND default_time_to_live = 2592000;
-- anomaly_type: 'VOLUME_SPIKE' | 'PRICE_JUMP' | 'STALE_TICK'