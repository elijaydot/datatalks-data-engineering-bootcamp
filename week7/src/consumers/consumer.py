"""
Console consumer: reads taxi rides from Kafka and prints them.
Good for verifying the producer is working before touching PostgreSQL.

Run with:
    uv run python src/consumers/consumer.py
"""

import sys
import os
from datetime import datetime

from kafka import KafkaConsumer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from models import ride_deserializer

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME   = 'rides'
GROUP_ID     = 'rides-console'
MAX_MESSAGES = 10           # stop after this many (set to None to run forever)
# ─────────────────────────────────────────────────────────────────────────────


def main():
    print(f"Connecting to Kafka at {KAFKA_SERVER}...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='earliest',   # read from the beginning of the topic
        group_id=GROUP_ID,
        value_deserializer=ride_deserializer,
    )

    print(f"Listening to topic '{TOPIC_NAME}' (group: {GROUP_ID})...\n")

    count = 0
    try:
        for message in consumer:
            ride = message.value
            pickup_dt = datetime.fromtimestamp(ride.tpep_pickup_datetime / 1000)
            print(
                f"[{count+1:>4}] "
                f"PU={ride.PULocationID:>3}  DO={ride.DOLocationID:>3}  "
                f"dist={ride.trip_distance:>5.2f}mi  "
                f"amt=${ride.total_amount:>6.2f}  "
                f"pickup={pickup_dt}"
            )
            count += 1
            if MAX_MESSAGES and count >= MAX_MESSAGES:
                print(f"\nStopped after {count} messages (MAX_MESSAGES={MAX_MESSAGES}).")
                break
    except KeyboardInterrupt:
        print(f"\nStopped. Received {count} messages total.")
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
