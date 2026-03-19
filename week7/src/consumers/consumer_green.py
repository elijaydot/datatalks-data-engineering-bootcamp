"""
Green taxi consumer for homework Q3.
Counts trips where trip_distance > 5.0
"""

import json
from kafka import KafkaConsumer

KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME   = 'green-trips'

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        # consumer_timeout_ms tells it to stop when no new messages for 5 seconds
        consumer_timeout_ms=5000,
    )

    total   = 0
    over_5  = 0

    for message in consumer:
        trip = message.value
        total += 1
        if trip.get('trip_distance', 0) > 5.0:
            over_5 += 1

    consumer.close()
    print(f"Total trips:          {total}")
    print(f"Trips with dist > 5:  {over_5}")

if __name__ == '__main__':
    main()