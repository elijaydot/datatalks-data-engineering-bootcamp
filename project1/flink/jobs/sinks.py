import logging
import duckdb
import json
import os
import time
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# GCS export function
# ─────────────────────────────────────────

class GCSSink:
    """
    Exports completed OHLCV candles to GCS data lake as JSON.
    Acts as the cloud data lake layer of the pipeline.
    """
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.enabled = bucket_name != ""
        if self.enabled:
            try:
                from google.cloud import storage
                self.client = storage.Client()
                self.bucket = self.client.bucket(bucket_name)
                logger.info(f"GCS sink connected to bucket: {bucket_name}")
            except Exception as e:
                logger.warning(f"GCS not available, skipping cloud export: {e}")
                self.enabled = False

    def write_ohlcv(self, ohlcv: dict):
        if not self.enabled:
            return
        try:
            ts = datetime.fromisoformat(ohlcv["window_start"])
            path = f"ohlcv/{ts.strftime('%Y/%m/%d')}/{ohlcv['symbol'].replace('/','-')}_{ts.strftime('%H%M')}.json"
            blob = self.bucket.blob(path)
            blob.upload_from_string(json.dumps(ohlcv))
        except Exception as e:
            logger.error(f"GCS write failed: {e}")

    def write_anomaly(self, anomaly: dict):
        if not self.enabled:
            return
        try:
            ts = datetime.fromisoformat(anomaly["timestamp"].replace("Z", "+00:00"))
            path = f"anomalies/{ts.strftime('%Y/%m/%d')}/{anomaly['symbol'].replace('/','-')}_{ts.strftime('%H%M%S')}.json"
            blob = self.bucket.blob(path)
            blob.upload_from_string(json.dumps(anomaly))
        except Exception as e:
            logger.error(f"GCS anomaly write failed: {e}")

# ─────────────────────────────────────────
# CASSANDRA SINK
# ─────────────────────────────────────────

class CassandraSink:
    def __init__(self, host: str = "cassandra", port: int = 9042):
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.cluster = Cluster(
                    [host],
                    port=port,
                    load_balancing_policy=RoundRobinPolicy(),
                    protocol_version=5,
                )
                self.session = self.cluster.connect("crypto")
                self._prepare_statements()
                logger.info("Connected to Cassandra successfully")
                return
            except Exception as e:
                logger.warning(f"Cassandra connection attempt {attempt+1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(10)
                else:
                    raise

    def _prepare_statements(self):
        """Pre-compile CQL statements for performance."""
        self.insert_tick = self.session.prepare("""
            INSERT INTO price_ticks
                (symbol, timestamp, price, volume, exchange, bid, ask)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """)

        self.insert_ohlcv = self.session.prepare("""
            INSERT INTO ohlcv_1min
                (symbol, window_start, open, high, low, close, volume, tick_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)

        self.insert_anomaly = self.session.prepare("""
            INSERT INTO anomalies
                (symbol, timestamp, price, volume, anomaly_type, anomaly_detail)
            VALUES (?, ?, ?, ?, ?, ?)
        """)

    def write_tick(self, tick: dict):
        try:
            self.session.execute(self.insert_tick, (
                tick["symbol"],
                datetime.fromisoformat(tick["timestamp"].replace("Z", "+00:00")),
                tick["price"],
                tick["volume"],
                tick["exchange"],
                tick["bid"],
                tick["ask"],
            ))
        except Exception as e:
            logger.error(f"Failed to write tick to Cassandra: {e}")

    def write_ohlcv(self, ohlcv: dict):
        try:
            self.session.execute(self.insert_ohlcv, (
                ohlcv["symbol"],
                datetime.fromisoformat(ohlcv["window_start"]),
                ohlcv["open"],
                ohlcv["high"],
                ohlcv["low"],
                ohlcv["close"],
                ohlcv["volume"],
                ohlcv["tick_count"],
            ))
        except Exception as e:
            logger.error(f"Failed to write OHLCV to Cassandra: {e}")

    def write_anomaly(self, anomaly: dict):
        try:
            self.session.execute(self.insert_anomaly, (
                anomaly["symbol"],
                datetime.fromisoformat(anomaly["timestamp"].replace("Z", "+00:00")),
                anomaly["price"],
                anomaly["volume"],
                anomaly["anomaly_type"],
                anomaly["anomaly_detail"],
            ))
            logger.warning(
                f"Anomaly detected: {anomaly['anomaly_type']} "
                f"for {anomaly['symbol']}"
            )
        except Exception as e:
            logger.error(f"Failed to write anomaly to Cassandra: {e}")

    def close(self):
        self.cluster.shutdown()


# ─────────────────────────────────────────
# DUCKDB SINK
# ─────────────────────────────────────────

class DuckDBSink:
    def __init__(self, path: str = "/data/crypto_history.duckdb"):
        self.conn = duckdb.connect(path)
         # Allow external read-only connections simultaneously
        self.conn.execute("PRAGMA enable_checkpoint_on_shutdown")
        self.conn.execute("SET checkpoint_threshold='10MB'")
        self._init_schema()
        logger.info(f"Connected to DuckDB at {path}")

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv (
                symbol       VARCHAR,
                window_start TIMESTAMP,
                open         DOUBLE,
                high         DOUBLE,
                low          DOUBLE,
                close        DOUBLE,
                volume       DOUBLE,
                tick_count   INTEGER,
                PRIMARY KEY (symbol, window_start)
            )
        """)

    def write_ohlcv(self, ohlcv: dict):
        """Write a 1-minute OHLCV candle to DuckDB."""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO ohlcv
                    (symbol, window_start, open, high, low,
                     close, volume, tick_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                ohlcv["symbol"],
                ohlcv["window_start"],
                ohlcv["open"],
                ohlcv["high"],
                ohlcv["low"],
                ohlcv["close"],
                ohlcv["volume"],
                ohlcv["tick_count"],
            ])
        except Exception as e:
            logger.error(f"Failed to write OHLCV to DuckDB: {e}")

    def close(self):
        self.conn.close()