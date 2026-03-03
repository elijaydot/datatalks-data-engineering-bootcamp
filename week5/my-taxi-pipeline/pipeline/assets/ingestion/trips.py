"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

depends:
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
  - name: pickup_location_id
    type: integer
  - name: dropoff_location_id
    type: integer
  - name: fare_amount
    type: double
  - name: payment_type
    type: bigint
  - name: taxi_type
    type: string
@bruin"""

import os
import json
import pandas as pd


TLC_LAST_AVAILABLE_MONTH_START = pd.Timestamp("2025-11-01", tz="UTC")


def _normalize_datetime_columns(df: pd.DataFrame) -> None:
  for column in ("pickup_datetime", "dropoff_datetime"):
    if column in df.columns:
      df[column] = pd.to_datetime(df[column], utc=True, errors="coerce")


def _get_pipeline_var(var_name: str, default_value):
  return json.loads(os.environ["BRUIN_VARS"]).get(var_name, default_value)


def _build_months_to_process(start_date_str: str, end_date_str: str) -> list[str]:
  max_months_per_run = int(_get_pipeline_var("max_months_per_run", 3))

  start_ts = pd.Timestamp(start_date_str, tz="UTC")
  end_ts = pd.Timestamp(end_date_str, tz="UTC")

  if end_ts > TLC_LAST_AVAILABLE_MONTH_START:
    print(
      f"Requested end date {end_ts.date()} is beyond available TLC data; "
      f"capping to {TLC_LAST_AVAILABLE_MONTH_START.date()}."
    )
    end_ts = TLC_LAST_AVAILABLE_MONTH_START

  months = pd.date_range(start_ts, end_ts, freq="MS").strftime("%Y-%m").tolist()

  if len(months) > max_months_per_run:
    raise ValueError(
      f"Requested {len(months)} months, but this asset allows at most {max_months_per_run} "
      "months per run to avoid out-of-memory errors. "
      "Please run a smaller interval (1-3 months recommended), or set pipeline variable "
      "`max_months_per_run` to a higher value if your machine has enough memory."
    )

  return months

def materialize():
    # Fix for Windows PyArrow issue where a custom timezone path might be set incorrectly
    if "PYARROW_TIMEZONE_DB_PATH" in os.environ:
        del os.environ["PYARROW_TIMEZONE_DB_PATH"]

    start_date_str = os.environ["BRUIN_START_DATE"]
    end_date_str = os.environ["BRUIN_END_DATE"]
    taxi_types = _get_pipeline_var("taxi_types", ["yellow"])

    # Generate a list of months to process based on the date range
    months = _build_months_to_process(start_date_str, end_date_str)

    all_trips_dfs = []
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    print(f"Fetching data for taxi types: {taxi_types} for months: {months}")

    for taxi_type in taxi_types:
        for month_str in months:
            year, month_num = month_str.split('-')
            url = f"{base_url}/{taxi_type}_tripdata_{year}-{month_num}.parquet"

            try:
                print(f"Reading data from: {url}")
                df = pd.read_parquet(url)

                # Add taxi_type column for identification
                df['taxi_type'] = taxi_type

                # Standardize column names (e.g., tpep_pickup_datetime -> pickup_datetime)
                if 'tpep_pickup_datetime' in df.columns:
                    df.rename(columns={
                        'tpep_pickup_datetime': 'pickup_datetime',
                        'tpep_dropoff_datetime': 'dropoff_datetime'
                    }, inplace=True)
                elif 'lpep_pickup_datetime' in df.columns:
                    df.rename(columns={
                        'lpep_pickup_datetime': 'pickup_datetime',
                        'lpep_dropoff_datetime': 'dropoff_datetime'
                    }, inplace=True)

                df.rename(columns={
                    'PULocationID': 'pickup_location_id',
                    'DOLocationID': 'dropoff_location_id'
                }, inplace=True)

                _normalize_datetime_columns(df)

                df = df[[
                  'pickup_datetime',
                  'dropoff_datetime',
                  'pickup_location_id',
                  'dropoff_location_id',
                  'fare_amount',
                  'payment_type',
                  'taxi_type',
                ]]

                all_trips_dfs.append(df)
            except Exception as e:
                print(f"Warning: Could not fetch or process data from {url}. Error: {e}")

    if not all_trips_dfs:
        print("Warning: No data was fetched. Returning an empty DataFrame.")
        return pd.DataFrame(columns=[
            'pickup_datetime', 'dropoff_datetime', 'pickup_location_id',
            'dropoff_location_id', 'fare_amount', 'payment_type', 'taxi_type'
        ])

    # Concatenate all dataframes into a single one
    final_dataframe = pd.concat(all_trips_dfs, ignore_index=True)

    # Ensure all required columns exist to provide a consistent schema
    required_columns = [
        'pickup_datetime', 'dropoff_datetime', 'pickup_location_id',
        'dropoff_location_id', 'fare_amount', 'payment_type', 'taxi_type'
    ]

    # Select and return only the columns defined in the @bruin block
    return final_dataframe[required_columns]
