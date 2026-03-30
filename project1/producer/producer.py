import json
import logging
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from schemas import CryptoTick
from prometheus_client import Counter

logger = logging.getLogger(__name__)

# Prometheus metrics
MESSAGES_SENT = Counter(
    "producer_messages_sent_total",
    "Total messages successfully sent to Redpanda",
    ["symbol"]
)
MESSAGES_FAILED = Counter(
    "producer_messages_failed_total",
    "Total messages that failed to send",
    ["symbol"]
)


class CryptoProducer:
    def __init__(self, broker: str, topic: str):
        self.topic = topic
        self.producer = Producer({
            "bootstrap.servers": broker,
            "client.id":         "crypto-producer",
            "acks":              "all",       # wait for all replicas
            "retries":           3,
            "retry.backoff.ms":  500,
        })
        self._ensure_topic_exists(broker, topic)

    def _ensure_topic_exists(self, broker: str, topic: str):
        """Create the Redpanda topic if it doesn't exist yet."""
        admin = AdminClient({"bootstrap.servers": broker})
        topics = admin.list_topics(timeout=10).topics
        if topic not in topics:
            admin.create_topics([
                NewTopic(topic, num_partitions=3, replication_factor=1)
            ])
            logger.info(f"Created topic: {topic}")

    def _delivery_report(self, err, msg):
        """Called by Kafka client after each message delivery attempt."""
        symbol = msg.key().decode() if msg.key() else "unknown"
        if err:
            logger.error(f"Delivery failed for {symbol}: {err}")
            MESSAGES_FAILED.labels(symbol=symbol).inc()
        else:
            logger.debug(
                f"Delivered {symbol} to "
                f"{msg.topic()}[{msg.partition()}] "
                f"offset {msg.offset()}"
            )
            MESSAGES_SENT.labels(symbol=symbol).inc()

    def publish(self, tick: CryptoTick):
        """Serialize a validated tick and send it to Redpanda."""
        try:
            self.producer.produce(
                topic=self.topic,
                key=tick.symbol.encode("utf-8"),
                value=json.dumps(tick.to_dict()).encode("utf-8"),
                on_delivery=self._delivery_report,
            )
            self.producer.poll(0)   # trigger delivery callbacks non-blocking
        except Exception as e:
            logger.error(f"Failed to produce {tick.symbol}: {e}")
            MESSAGES_FAILED.labels(symbol=tick.symbol).inc()

    def flush(self):
        """Wait for all in-flight messages to be delivered."""
        self.producer.flush()