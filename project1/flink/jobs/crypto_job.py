import os
import time
import json
import logging
from datetime import datetime, timezone
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError
from transformations import parse_tick, is_stale, normalize_tick, detect_anomaly
from sinks import CassandraSink, DuckDBSink, GCSSink
from prometheus_client import Gauge, start_http_server

CRYPTO_PRICE  = Gauge('crypto_price',  'Latest crypto price',  ['symbol'])
CRYPTO_VOLUME = Gauge('crypto_volume', 'Latest crypto volume', ['symbol'])


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# OHLCV WINDOW AGGREGATOR
# ─────────────────────────────────────────

class OHLCVAggregator:
    """
    Accumulates ticks per symbol per 1-minute window.
    Flushes completed windows to Cassandra and DuckDB.
    """
    def __init__(self):
        self.windows = defaultdict(lambda: defaultdict(list))

    def _window_key(self, timestamp: str) -> str:
        """Truncate timestamp to the current minute."""
        ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return ts.strftime("%Y-%m-%dT%H:%M:00+00:00")

    def add(self, tick: dict):
        symbol = tick["symbol"]
        window = self._window_key(tick["timestamp"])
        self.windows[symbol][window].append(tick["price"])

    def flush_completed(self, current_time: datetime) -> list[dict]:
        """
        Return completed 1-min windows (i.e. not the current minute).
        Remove them from memory after flushing.
        """
        current_window = current_time.strftime("%Y-%m-%dT%H:%M:00+00:00")
        completed = []

        for symbol in list(self.windows.keys()):
            for window_key in list(self.windows[symbol].keys()):
                if window_key < current_window:
                    prices = self.windows[symbol].pop(window_key)
                    if prices:
                        completed.append({
                            "symbol":       symbol,
                            "window_start": window_key,
                            "open":         prices[0],
                            "high":         max(prices),
                            "low":          min(prices),
                            "close":        prices[-1],
                            "volume":       0.0,  # summed separately below
                            "tick_count":   len(prices),
                        })

        return completed


# ─────────────────────────────────────────
# MAIN JOB
# ─────────────────────────────────────────

def main():
    broker       = os.getenv("REDPANDA_BROKER",  "redpanda:9092")
    topic        = os.getenv("KAFKA_TOPIC",       "crypto-raw-ticks")
    cassandra_host = os.getenv("CASSANDRA_HOST",  "cassandra")
    duckdb_path  = os.getenv("DUCKDB_PATH",       "/data/crypto_history.duckdb")
    group_id     = os.getenv("KAFKA_GROUP_ID",    "flink-consumer")

    # ── Wait for dependencies ───────────────────────────────
    logger.info("Waiting 20s for Cassandra and Redpanda to be ready...")
    time.sleep(40)

    start_http_server(8001)
    logger.info("Flink job Prometheus metrics server started on port 8001")

    # ── Connect to sinks ────────────────────────────────────
    cassandra = CassandraSink(host=cassandra_host)
    duckdb    = DuckDBSink(path=duckdb_path)
    gcs = GCSSink(bucket_name=os.getenv("GCS_BUCKET", ""))

    # ── Kafka consumer ──────────────────────────────────────
    consumer = Consumer({
        "bootstrap.servers":  broker,
        "group.id":           group_id,
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([topic])
    logger.info(f"Subscribed to topic: {topic}")

    # ── State tracking ──────────────────────────────────────
    prev_ticks  = {}          # last tick per symbol for anomaly detection
    aggregator  = OHLCVAggregator()

    # ── Main processing loop ────────────────────────────────
    logger.info("Flink job started — processing stream...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
            else:
                raw = msg.value().decode("utf-8")

                # 1. Parse
                tick = parse_tick(raw)
                if tick is None:
                    continue

                # 2. Filter stale
                if is_stale(tick):
                    logger.debug(f"Dropping stale tick: {tick['symbol']}")
                    continue

                # 3. Normalize
                tick = normalize_tick(tick)

                CRYPTO_PRICE.labels(symbol=tick['symbol']).set(tick['price'])
                CRYPTO_VOLUME.labels(symbol=tick['symbol']).set(tick['volume'])

                # 4. Detect anomalies
                symbol = tick["symbol"]
                anomaly = detect_anomaly(tick, prev_ticks.get(symbol))
                if anomaly:
                    cassandra.write_anomaly(anomaly)
                    gcs.write_anomaly(anomaly)

                # 5. Write tick to Cassandra
                cassandra.write_tick(tick)
                # gcs.write_tick(tick)

                # 6. Accumulate into OHLCV window
                aggregator.add(tick)

                # 7. Update previous tick state
                prev_ticks[symbol] = tick

                logger.info(
                    f"Processed {symbol} | "
                    f"${tick['price']:,.2f} | "
                    f"vol={tick['volume']:.4f}"
                )

            # 8. Flush completed OHLCV windows every cycle
            completed_windows = aggregator.flush_completed(
                datetime.now(timezone.utc)
            )
            for ohlcv in completed_windows:
                cassandra.write_ohlcv(ohlcv)
                duckdb.write_ohlcv(ohlcv)
                gcs.write_ohlcv(ohlcv)
                logger.info(
                    f"OHLCV flushed: {ohlcv['symbol']} | "
                    f"window={ohlcv['window_start']} | "
                    f"ticks={ohlcv['tick_count']}"
                )

    except KeyboardInterrupt:
        logger.info("Shutting down Flink job...")
    finally:
        consumer.close()
        cassandra.close()
        duckdb.close()


if __name__ == "__main__":
    main()