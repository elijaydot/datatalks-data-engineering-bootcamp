"""
Green taxi producer for homework.
Sends green_tripdata_2025-10.parquet to the 'green-trips' Kafka topic.
"""

import json
import pandas as pd
from kafka import KafkaProducer
from time import time

KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME   = 'green-trips'
DATA_URL     = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
COLUMNS      = [
    'lpep_pickup_datetime', 'lpep_dropoff_datetime',
    'PULocationID', 'DOLocationID',
    'passenger_count', 'trip_distance', 'tip_amount', 'total_amount'
]

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def main():
    print("Downloading green taxi data...")
    df = pd.read_parquet(DATA_URL, columns=COLUMNS)
    print(f"Loaded {len(df)} rows.")

    # Convert datetime columns to strings (can't JSON-serialize pandas Timestamps)
    df['lpep_pickup_datetime']  = df['lpep_pickup_datetime'].astype(str)
    df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].astype(str)

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=json_serializer,
    )

    # First create the topic (or it auto-creates on first send)
    print(f"Sending to topic '{TOPIC_NAME}'...")

    t0 = time()

    for _, row in df.iterrows():
        record = row.to_dict()
        # Replace NaN values with None (serializes to JSON null, not NaN)
        record = {k: (None if isinstance(v, float) and v != v else v) for k, v in record.items()}
        producer.send(TOPIC_NAME, value=record)

    producer.flush()

    t1 = time()
    print(f'took {(t1 - t0):.2f} seconds')

if __name__ == '__main__':
    main()