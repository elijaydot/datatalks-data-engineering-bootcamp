"""
Shared data model for the streaming pipeline.
Imported by producers, consumers, and Flink jobs.
"""

import json
from dataclasses import dataclass


@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    tpep_pickup_datetime: int  # epoch milliseconds


def ride_from_row(row):
    """Convert a pandas DataFrame row into a Ride dataclass."""
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        # Convert pandas Timestamp -> epoch milliseconds (Flink expects this format)
        tpep_pickup_datetime=int(row['tpep_pickup_datetime'].timestamp() * 1000),
    )


def ride_serializer(ride):
    """Serialize a Ride dataclass to JSON bytes for Kafka."""
    import dataclasses
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')


def ride_deserializer(data):
    """Deserialize JSON bytes from Kafka back into a Ride dataclass."""
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)
