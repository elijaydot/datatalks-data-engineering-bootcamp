import pandas as pd
from sqlalchemy import create_engine
from time import time

# Database connection parameters
db_user = 'postgres'
db_password = 'postgres'
db_host = 'localhost'  # We're connecting from host machine
db_port = '5433'  # Host port from docker-compose
db_name = 'ny_taxi'

# Create connection string
connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

# Create engine
engine = create_engine(connection_string)

print("=" * 60)
print("Starting data loading process...")
print("=" * 60)

# ============================================================
# Load Green Taxi Data in Chunks
# ============================================================
print("\n[1/2] Loading green taxi data in chunks...")
print("-" * 60)

parquet_file = 'green_tripdata_2025-11.parquet'
chunk_size = 100000  # Load 100,000 rows at a time

# Read parquet file
df_iterator = pd.read_parquet(parquet_file, engine='pyarrow')

# Get total rows for progress tracking
total_rows = len(df_iterator)
print(f"Total rows to load: {total_rows:,}")

# Split dataframe into chunks and load
total_chunks = (total_rows // chunk_size) + (1 if total_rows % chunk_size != 0 else 0)
print(f"Processing in {total_chunks} chunks of {chunk_size:,} rows each\n")

t_start = time()

for i, start_idx in enumerate(range(0, total_rows, chunk_size), 1):
    t_chunk_start = time()
    
    # Get chunk
    end_idx = min(start_idx + chunk_size, total_rows)
    chunk = df_iterator.iloc[start_idx:end_idx]
    
    # Load chunk to database
    if i == 1:
        # First chunk: replace table if exists
        chunk.to_sql('green_taxi_trips', engine, if_exists='replace', index=False)
        print(f"✓ Chunk {i}/{total_chunks}: Created table and loaded rows {start_idx+1:,} to {end_idx:,}")
    else:
        # Subsequent chunks: append to table
        chunk.to_sql('green_taxi_trips', engine, if_exists='append', index=False)
        print(f"✓ Chunk {i}/{total_chunks}: Loaded rows {start_idx+1:,} to {end_idx:,}")
    
    t_chunk_end = time()
    chunk_time = t_chunk_end - t_chunk_start
    print(f"  Time taken: {chunk_time:.2f} seconds")

t_end = time()
total_time = t_end - t_start

print(f"\n{'='*60}")
print(f"Green taxi data loading complete!")
print(f"Total rows loaded: {total_rows:,}")
print(f"Total time: {total_time:.2f} seconds")
print(f"Average speed: {total_rows/total_time:,.0f} rows/second")
print(f"{'='*60}")

# ============================================================
# Load Taxi Zone Data (Small file, no chunking needed)
# ============================================================
print("\n[2/2] Loading taxi zone lookup data...")
print("-" * 60)

t_start = time()

df_zones = pd.read_csv('taxi_zone_lookup.csv')
df_zones.to_sql('taxi_zones', engine, if_exists='replace', index=False)

t_end = time()
zone_time = t_end - t_start

print(f"✓ Loaded {len(df_zones):,} rows into taxi_zones table")
print(f"  Time taken: {zone_time:.2f} seconds")

print("\n" + "=" * 60)
print("ALL DATA LOADING COMPLETE! ✓")
print("=" * 60)
print("\nYou can now run queries on the following tables:")
print("  - green_taxi_trips")
print("  - taxi_zones")