"""
PostgreSQL consumer: reads taxi rides from Kafka and inserts them into
the processed_events table in PostgreSQL.

Requires the table to exist first — run this SQL in pgcli:
    CREATE TABLE processed_events (
        PULocationID INTEGER,
        DOLocationID INTEGER,
        trip_distance DOUBLE PRECISION,
        total_amount DOUBLE PRECISION,
        pickup_datetime TIMESTAMP
    );

Run with:
    uv run python src/consumers/consumer_postgres.py
"""

import sys
import os
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from models import ride_deserializer

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME   = 'rides'
GROUP_ID     = 'rides-to-postgres'   # separate group from console consumer

PG_HOST      = 'localhost'
PG_PORT      = 5432
PG_DB        = 'postgres'
PG_USER      = 'postgres'
PG_PASSWORD  = 'postgres'
# ─────────────────────────────────────────────────────────────────────────────


def main():
    # Connect to PostgreSQL
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        database=PG_DB, user=PG_USER, password=PG_PASSWORD,
    )
    conn.autocommit = True   # commit each INSERT immediately, no manual commit needed
    cur = conn.cursor()
    print("Connected to PostgreSQL.\n")

    # Connect to Kafka
    print(f"Connecting to Kafka at {KAFKA_SERVER}...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='earliest',
        group_id=GROUP_ID,
        value_deserializer=ride_deserializer,
    )
    print(f"Listening to topic '{TOPIC_NAME}'...\n")
    print("Press Ctrl+C to stop.\n")

    count = 0
    try:
        for message in consumer:
            ride = message.value
            pickup_dt = datetime.fromtimestamp(ride.tpep_pickup_datetime / 1000)

            cur.execute(
                """INSERT INTO processed_events
                   (PULocationID, DOLocationID, trip_distance, total_amount, pickup_datetime)
                   VALUES (%s, %s, %s, %s, %s)""",
                (ride.PULocationID, ride.DOLocationID,
                 ride.trip_distance, ride.total_amount, pickup_dt),
            )
            count += 1

            if count % 100 == 0:
                print(f"Inserted {count} rows...")

    except KeyboardInterrupt:
        print(f"\nStopped. Inserted {count} rows total.")
    finally:
        consumer.close()
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
