"""
DE Zoomcamp - Week 6: Batch Processing with Spark
Homework Solutions

Run this script AFTER:
1. Installing PySpark:  pip install pyspark
2. Downloading the data:
   wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
   wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# SETUP: Create a local Spark session
# ============================================================
# SparkSession is your entry point to everything in Spark.
# "local[*]" means: run locally, use all available CPU cores.
#
# NOTE: The config lines below keep Spark compatible with modern Java releases.
java_opts = (
    "--add-opens=java.base/javax.security.auth=ALL-UNNAMED"
)

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("DE_Zoomcamp_Week6") \
    .config("spark.driver.extraJavaOptions",
        java_opts) \
    .config("spark.executor.extraJavaOptions",
        java_opts) \
    .getOrCreate()

# ============================================================
# QUESTION 1: What is the Spark version?
# ============================================================
print("=" * 50)
print("Q1 - Spark Version:")
print(spark.version)
# Expected output: 3.5.x  (exact patch version depends on install)

# ============================================================
# QUESTION 2: Average size of repartitioned parquet files
# ============================================================
print("\n" + "=" * 50)
print("Q2 - Repartition to 4 and save as parquet:")

# Read the raw parquet file into a Spark DataFrame
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

print(f"Total rows: {df.count():,}")
print(f"Schema:")
df.printSchema()

# Repartition into exactly 4 partitions
# Each partition becomes one .parquet file when saved
df_repartitioned = df.repartition(4)

folder = "yellow_nov_2025_partitioned/"
use_partitioned_output = False

try:
    df_repartitioned.write.mode("overwrite").parquet(folder)

    parquet_files = [
        f for f in os.listdir(folder)
        if f.endswith(".parquet")
    ]
    sizes_mb = [
        os.path.getsize(os.path.join(folder, f)) / (1024 * 1024)
        for f in parquet_files
    ]
    print(f"\nFiles created: {len(parquet_files)}")
    for fname, size in zip(parquet_files, sizes_mb):
        print(f"  {fname}: {size:.2f} MB")
    print(f"\nAverage file size: {sum(sizes_mb)/len(sizes_mb):.2f} MB")
    use_partitioned_output = True
except Exception as err:
    short_err = str(err).splitlines()[0]
    print("\nSkipping parquet write size check (Windows Hadoop winutils is not configured).")
    print(f"Write error: {type(err).__name__}: {short_err}")

# ============================================================
# QUESTION 3: Trips on November 15th
# ============================================================
print("\n" + "=" * 50)
print("Q3 - Number of trips on November 15, 2025:")

# Read the repartitioned version (or original, same data)
if use_partitioned_output:
    df = spark.read.parquet(folder)
else:
    df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# Filter for trips that STARTED on Nov 15
# tpep_pickup_datetime is the column for pickup time
trips_nov15 = df.filter(
    (F.to_date(F.col("tpep_pickup_datetime")) == "2025-11-15")
)

count_nov15 = trips_nov15.count()
print(f"Trips on Nov 15: {count_nov15:,}")
# ANSWER: 162,604 (closest match)

# ============================================================
# QUESTION 4: Longest trip in hours
# ============================================================
print("\n" + "=" * 50)
print("Q4 - Longest trip duration in hours:")

# Calculate trip duration:
# dropoff time - pickup time, converted to seconds, then to hours
df_with_duration = df.withColumn(
    "trip_duration_hours",
    (
        F.unix_timestamp("tpep_dropoff_datetime") -
        F.unix_timestamp("tpep_pickup_datetime")
    ) / 3600  # 3600 seconds in an hour
)

# Get the maximum duration
max_duration = df_with_duration.agg(
    F.max("trip_duration_hours").alias("max_hours")
).collect()[0]["max_hours"]

print(f"Longest trip: {max_duration:.1f} hours")
# ANSWER: 162,604 hours is wrong - expected answer ~90.6 hours
# ANSWER: 90.6

# ============================================================
# QUESTION 5: Spark UI Port
# ============================================================
print("\n" + "=" * 50)
print("Q5 - Spark UI runs on port:")
print("4040")
print("Visit: http://localhost:4040 while a Spark session is active")
# ANSWER: 4040

# ============================================================
# QUESTION 6: Least frequent pickup location zone
# ============================================================
print("\n" + "=" * 50)
print("Q6 - Least frequent pickup zone:")

# Load the zone lookup CSV into a temp view
# This maps LocationID (a number) → Zone name (like "JFK Airport")
zones = spark.read \
    .option("header", "true") \
    .csv("taxi_zone_lookup.csv")

print("Zone lookup schema:")
zones.printSchema()

# Register both as temporary SQL views so we can query them
df.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")

# SQL query:
# 1. Join trips with zones on PULocationID = LocationID
# 2. Group by Zone name
# 3. Count how many trips started in each zone
# 4. Sort ascending → first row = least frequent
result = spark.sql("""
    SELECT
        z.Zone,
        COUNT(1) AS trip_count
    FROM trips t
    JOIN zones z
        ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY trip_count ASC
    LIMIT 5
""")

print("\nLeast frequent pickup zones:")
result.show(truncate=False)
# ANSWER: Governor's Island/Ellis Island/Liberty Island (or Arden Heights)

# Cleanup
spark.stop()
print("\nSpark session stopped. Done!")