"""
Producer: reads 1000 NYC Yellow Taxi trips from a parquet file
and sends them to Kafka (Redpanda) one by one.

Run with:
    uv run python src/producers/producer.py
"""

import dataclasses
import sys
import os
import time
from pathlib import Path


import pandas as pd
from kafka import KafkaProducer

# Add src/ to path so we can import models.py
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from models import Ride, ride_from_row, ride_serializer

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_SERVER = 'localhost:9092'   # external port (we're running outside Docker)
TOPIC_NAME   = 'rides'
DATA_URL     = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
COLUMNS      = ['PULocationID', 'DOLocationID', 'trip_distance', 'total_amount', 'tpep_pickup_datetime']
NUM_ROWS     = 1000
DELAY_SEC    = 0.01               # 10ms between messages
# ─────────────────────────────────────────────────────────────────────────────


def main():
    print("Downloading taxi data...")
    df = pd.read_parquet(DATA_URL, columns=COLUMNS).head(NUM_ROWS)
    print(f"Loaded {len(df)} rows. Sample:")
    print(df.head(3))
    print()

    print(f"Connecting to Kafka at {KAFKA_SERVER}...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=ride_serializer,
    )

    print(f"Sending {NUM_ROWS} rides to topic '{TOPIC_NAME}'...\n")
    t0 = time.time()

    for _, row in df.iterrows():
        ride = ride_from_row(row)
        producer.send(TOPIC_NAME, value=ride)
        print(f"Sent: {ride}")
        time.sleep(DELAY_SEC)

    producer.flush()
    t1 = time.time()
    print(f"\nDone! Sent {NUM_ROWS} rides in {(t1 - t0):.2f} seconds.")


if __name__ == '__main__':
    main()
