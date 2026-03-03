import duckdb
import os
import requests
from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

CAST_COLUMNS = {
    "yellow": {
        "congestion_surcharge": "DOUBLE",
        "ehail_fee": "DOUBLE",
        "airport_fee": "DOUBLE",
    },
    "green": {
        "congestion_surcharge": "DOUBLE",
        "ehail_fee": "DOUBLE",
    },
    "fhv": {
        "sr_flag": "INTEGER",
        "SR_Flag": "INTEGER",
    },
}

def download_and_convert_files(taxi_type, years):
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in years:
        for month in range(1, 13):
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            # Download CSV.gz file
            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            response = requests.get(f"{BASE_URL}/{taxi_type}/{csv_gz_filename}", stream=True)
            response.raise_for_status()

            with open(csv_gz_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"Converting {csv_gz_filename} to Parquet...")
            con = duckdb.connect()
            con.execute(f"""
                COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}'))
                TO '{parquet_filepath}' (FORMAT PARQUET)
            """)
            con.close()

            # Remove the CSV.gz file to save space
            csv_gz_filepath.unlink()
            print(f"Completed {parquet_filename}")

def update_gitignore():
    gitignore_path = Path(".gitignore")
    entries_to_ignore = {
        "data/": "Data directory",
        "dbt_packages/": "dbt packages",
        "logs/": "dbt logs",
    }

    for entry, comment in entries_to_ignore.items():
        # We must read the file each time to check its current state before appending
        content = gitignore_path.read_text() if gitignore_path.exists() else ""
        if entry not in content:
            with open(gitignore_path, 'a') as f:
                f.write(f"\n# {comment}\n{entry}\n" if content else f"# {comment}\n{entry}\n")


def ensure_bigquery_dataset(client, project_id, dataset_id, location="US"):
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset exists: {dataset_ref}")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
        print(f"Created dataset: {dataset_ref}")


def normalize_parquet_schema(parquet_filepath, taxi_type):
    cast_columns = CAST_COLUMNS.get(taxi_type, {})
    if not cast_columns:
        return

    path = parquet_filepath.as_posix()
    con = duckdb.connect()
    try:
        columns = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}')"
        ).fetchall()
        column_names = {row[0] for row in columns}
        # Create case-insensitive lookup
        column_map = {col.lower(): col for col in column_names}

        replacements = []
        for cast_col_name, column_type in cast_columns.items():
            # Check both exact match and case-insensitive match
            actual_col_name = None
            if cast_col_name in column_names:
                actual_col_name = cast_col_name
            elif cast_col_name.lower() in column_map:
                actual_col_name = column_map[cast_col_name.lower()]
            
            if actual_col_name:
                replacements.append(
                    f"try_cast({actual_col_name} as {column_type}) as {actual_col_name}"
                )

        if not replacements:
            return

        replace_sql = ", ".join(replacements)
        con.execute(
            f"COPY (SELECT * REPLACE ({replace_sql}) FROM read_parquet('{path}')) "
            f"TO '{path}' (FORMAT PARQUET)"
        )
    finally:
        con.close()


def load_parquet_files_to_bigquery(client, project_id, dataset_id, taxi_type):
    parquet_dir = Path("data") / taxi_type
    parquet_files = sorted(parquet_dir.glob("*.parquet"))

    if not parquet_files:
        raise RuntimeError(f"No parquet files found in {parquet_dir}")

    table_id = f"{project_id}.{dataset_id}.{taxi_type}_tripdata"

    # Check if table already exists and has data
    try:
        table = client.get_table(table_id)
        if table.num_rows > 0:
            print(f"Skipping {table_id} (already exists with {table.num_rows} rows)")
            return
        else:
            # Table exists but empty, delete and recreate to avoid schema issues
            print(f"Deleting empty table {table_id} to ensure clean schema")
            client.delete_table(table_id)
    except NotFound:
        print(f"Table {table_id} does not exist, will create and load data")

    for index, parquet_file in enumerate(parquet_files):
        normalize_parquet_schema(parquet_file, taxi_type)

        write_disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if index == 0
            else bigquery.WriteDisposition.WRITE_APPEND
        )

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
            write_disposition=write_disposition,
        )

        with open(parquet_file, "rb") as source_file:
            load_job = client.load_table_from_file(
                source_file,
                table_id,
                job_config=job_config,
            )

        load_job.result()
        print(f"Loaded {parquet_file.name} -> {table_id}")

if __name__ == "__main__":
    # Update .gitignore to exclude data directory
    update_gitignore()

    # Download Yellow and Green taxi data for 2019-2020
    for taxi_type in ["yellow", "green"]:
        download_and_convert_files(taxi_type, years=[2019, 2020])
    
    # Download FHV data for 2019 only
    download_and_convert_files("fhv", years=[2019])

    gcp_project_id = os.getenv("GCP_PROJECT_ID")
    if not gcp_project_id:
        raise RuntimeError("Set GCP_PROJECT_ID in your environment before running this script.")

    bq_dataset = os.getenv("BQ_DATASET", "prod")
    bq_location = os.getenv("BQ_LOCATION", "US")

    bq_client = bigquery.Client(project=gcp_project_id)
    ensure_bigquery_dataset(bq_client, gcp_project_id, bq_dataset, bq_location)

    for taxi_type in ["yellow", "green", "fhv"]:
        load_parquet_files_to_bigquery(bq_client, gcp_project_id, bq_dataset, taxi_type)

    print("BigQuery load completed.")