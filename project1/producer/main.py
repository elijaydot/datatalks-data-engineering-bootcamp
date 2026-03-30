import os
import time
import logging
from prometheus_client import start_http_server
from fetcher import CryptoFetcher
from producer import CryptoProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    # ── Config from environment variables ──────────────────────
    broker   = os.getenv("REDPANDA_BROKER", "localhost:9092")
    topic    = os.getenv("KAFKA_TOPIC",     "crypto-raw-ticks")
    exchange = os.getenv("CRYPTO_EXCHANGE", "binance")
    symbols  = os.getenv(
        "CRYPTO_SYMBOLS", "BTC/USDT,ETH/USDT,SOL/USDT"
    ).split(",")
    interval = float(os.getenv("FETCH_INTERVAL_MS", "500")) / 1000

    # ── Start Prometheus metrics server ────────────────────────
    start_http_server(8000)
    logger.info("Prometheus metrics server started on port 8000")

    # ── Initialise fetcher and producer ────────────────────────
    fetcher  = CryptoFetcher(exchange_name=exchange, symbols=symbols)
    producer = CryptoProducer(broker=broker, topic=topic)

    logger.info(
        f"Starting producer | exchange={exchange} | "
        f"symbols={symbols} | interval={interval}s"
    )

    # ── Main loop ───────────────────────────────────────────────
    try:
        while True:
            ticks = fetcher.fetch_ticks()
            for tick in ticks:
                producer.publish(tick)
                logger.info(
                    f"{tick.symbol} | ${tick.price:,.2f} | "
                    f"vol={tick.volume:.4f}"
                )
            producer.flush()
            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
        producer.flush()


if __name__ == "__main__":
    main()