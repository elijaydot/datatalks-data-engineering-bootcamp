# Week 3: Data Warehousing & BigQuery

## Overview
This project explores data warehousing concepts using Google BigQuery and Cloud Storage. We worked with NYC Yellow Taxi trip data from January to June 2024, learning how to efficiently store, query, and optimize large datasets in the cloud.

## Dataset
**NYC Yellow Taxi Trip Records (Jan - June 2024)**
- Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Format: Parquet files
- Time period: 6 months (2024-01 to 2024-06)
- Total records: `20,332,093`

## Project Structure
```
week3_homework/
├── README.md
├── load_yellow_taxi_data.py      # Script to download and upload data to GCS
├── create_tables.sql              # SQL queries for table creation
├── homework_queries.sql           # SQL queries for homework questions
└── gcs.json                       # Service account credentials (not committed)
```

## Setup & Configuration

### Prerequisites
- Google Cloud Platform account
- Python 3.x installed
- `google-cloud-storage` package

### GCP Resources Created
- **Project ID**: `[YOUR_PROJECT_ID]`
- **GCS Bucket**: `[YOUR_BUCKET_NAME]`
- **BigQuery Dataset**: `yellow_taxi_data_wk3`
- **Region**: `[YOUR_REGION]`

### Installation
```bash
# Install required Python packages
pip install google-cloud-storage

# Set up authentication
export GOOGLE_APPLICATION_CREDENTIALS="path/to/gcs.json"
```

## Implementation Steps

### 1. Data Ingestion
Uploaded parquet files to Google Cloud Storage using the provided Python script:

```bash
python load_yellow_taxi_data.py
```

The script:
- Downloads 6 months of Yellow Taxi data in parallel
- Creates GCS bucket if it doesn't exist
- Uploads files to `gs://[YOUR_BUCKET_NAME]/`
- Verifies each upload with retry logic

**Result**: 6 parquet files successfully stored in GCS

### 2. External Table Creation
Created an external table that references the parquet files in GCS:

```sql
CREATE OR REPLACE EXTERNAL TABLE `yellow_taxi_data_wk3.external_yellow_taxi`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://[YOUR_BUCKET_NAME]/yellow_tripdata_2024-*.parquet']
);
```

**Key Concept**: External tables store metadata in BigQuery but keep actual data in GCS. This means:
- No data duplication
- Lower storage costs in BigQuery
- Slower query performance (reads from GCS each time)

### 3. Materialized Table Creation
Created a regular (materialized) table by copying data from the external table:

```sql
CREATE OR REPLACE TABLE `yellow_taxi_data_wk3.yellow_taxi_regular_table` AS
SELECT * FROM `yellow_taxi_data_wk3.external_yellow_taxi`;
```

**Key Concept**: Materialized tables store data in BigQuery's native columnar format:
- Faster query performance
- Enables partitioning and clustering
- Higher storage costs

### 4. Optimized Table Creation
Created a partitioned and clustered table for better query performance:

```sql
CREATE OR REPLACE TABLE `yellow_taxi_data_wk3.yellow_taxi_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `yellow_taxi_data_wk3.yellow_taxi_regular_table`;
```

**Optimization Strategy**:
- **Partitioning** by `tpep_dropoff_datetime`: Divides table into date-based segments, reducing data scanned for date-range queries
- **Clustering** by `VendorID`: Sorts data within partitions, improving performance for filters and ordering on VendorID

## Homework Questions & Answers

### Question 1: Total Record Count
**Query:**
```sql
SELECT COUNT(*) as record_count
FROM yellow_taxi_data_wk3.yellow_taxi_regular_table;
```

**Answer**: `20,332,093`

---

### Question 2: Data Scanning Estimates
**Queries:**
```sql
-- External Table
SELECT COUNT(*) 
FROM yellow_taxi_data_wk3.external_yellow_taxi;

-- Materialized Table
SELECT COUNT(*) 
FROM yellow_taxi_data_wk3.yellow_taxi_regular_table;
```

**Estimates**:
- External Table: `0 MB`
- Materialized Table: `0 MB`

**Answer**: `0 MB for the External Table and 0MB for the Materialized Table`

**Explanation**: The materialized table shows 0 MB because BigQuery stores row counts in metadata and doesn't need to scan data for `COUNT(*)`. The external table must scan the actual parquet files in GCS.

---

### Question 3: Columnar Storage Impact
**Queries:**
```sql
-- One column
SELECT PULocationID
FROM yellow_taxi_data_wk3.yellow_taxi_regular_table;

-- Two columns
SELECT PULocationID, DOLocationID
FROM yellow_taxi_data_wk3.yellow_taxi_regular_table;
```

**Estimates**:
- One column: `155.12 MB`
- Two columns: `310.24 MB`

**Answer**: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

**Insight**: This demonstrates BigQuery's cost efficiency - you only pay for the columns you actually query.

---

### Question 4: Zero Fare Records
**Query:**
```sql
SELECT COUNT(*) as zero_fare_count
FROM yellow_taxi_data_wk3.yellow_taxi_regular_table
WHERE fare_amount = 0;
```

**Answer**: `8,333`

---

### Question 5: Optimization Strategy
**Question**: What is the best strategy to optimize a table if queries always filter by `tpep_dropoff_datetime` and order by `VendorID`?

**Answer**: Partition by tpep_dropoff_datetime and Cluster on VendorID

**Rationale**:
- **Partition** on datetime columns used in WHERE clauses (reduces data scanned)
- **Cluster** on columns used in ORDER BY and filters (improves data organization)
- Can only partition by one column, but can cluster by up to four

---

### Question 6: Partitioning Benefits
**Query:**
```sql
SELECT DISTINCT VendorID
FROM [TABLE_NAME]
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```

**Estimates**:
- Non-partitioned table: `310.24 MB`
- Partitioned table: `26.84 MB`

**Answer**: `310.24 MB for non-partitioned table and 26.84 MB for the partitioned table`

**Insight**: Partitioning dramatically reduces data scanned. The non-partitioned table must scan all 6 months of data, while the partitioned table only scans March 1-15 partitions.

---

### Question 7: External Table Storage Location
**Answer**: GCP Bucket

**Explanation**: External tables are metadata pointers in BigQuery. The actual data remains in Google Cloud Storage (GCS buckets) and is read on-demand during queries.

---

### Question 8: Always Cluster Tables?
**Answer**: False

**Explanation**: Clustering is beneficial for large tables (>1 GB) with predictable query patterns on high-cardinality columns. For small tables, tables with unpredictable queries, or low-cardinality columns, clustering adds overhead without significant benefit.

**When to cluster**:
- Large tables with frequent filters on specific columns
- High-cardinality columns (many distinct values)
- Consistent query patterns

**When NOT to cluster**:
- Small tables (<1 GB)
- Full table scans
- Low-cardinality columns
- Unpredictable query patterns

---

### Question 9: COUNT(*) on Materialized Tables
**Query:**
```sql
SELECT COUNT(*)
FROM yellow_taxi_data_wk3.yellow_taxi_regular_table;
```

**Estimated Bytes**: `0 B`

**Answer**: 0 Bytes

**Why?**: BigQuery stores table metadata including row counts. For simple `COUNT(*)` queries on materialized tables, BigQuery retrieves the pre-computed count from metadata without scanning any data.

**Note**: This optimization doesn't apply to:
- `COUNT(column_name)` - must check for NULLs
- `COUNT(*) WHERE condition` - must filter data
- External tables - no metadata optimization

---

## Key Learnings

### External vs Materialized Tables
| Feature | External Table | Materialized Table |
|---------|---------------|-------------------|
| Data Location | GCS Bucket | BigQuery Storage |
| Query Speed | Slower | Faster |
| Storage Cost | GCS pricing only | BigQuery pricing |
| Partitioning/Clustering | Not supported | Supported |
| Metadata Optimization | Limited | Full support |

### BigQuery Optimization Techniques
1. **Partitioning**: Split tables by date/timestamp for efficient time-range queries
2. **Clustering**: Sort data within partitions for better filter performance
3. **Columnar Storage**: Only scan columns you need, reducing costs
4. **Metadata Optimization**: `COUNT(*)` queries use metadata, not data scans

### Cost Optimization
- Use external tables for infrequently queried data
- Partition large tables by date
- Cluster by frequently filtered columns
- Select only needed columns in queries
- Use `COUNT(*)` instead of `COUNT(column)` when possible

## Technologies Used
- **Google Cloud Storage**: Object storage for parquet files
- **Google BigQuery**: Serverless data warehouse
- **Python**: Data ingestion automation
- **SQL**: Data querying and analysis
- **Parquet**: Columnar storage format

## Challenges & Solutions

**Challenge**: Understanding the difference between external and materialized tables  
**Solution**: Ran comparison queries to observe differences in data scanning and performance

**Challenge**: Determining optimal partitioning and clustering strategy  
**Solution**: Analyzed query patterns and applied partitioning on filter columns, clustering on sort columns

**Challenge**: Interpreting BigQuery cost estimates  
**Solution**: Tested various queries to understand how BigQuery calculates data scanning

## Resources
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Parquet File Format](https://parquet.apache.org/)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices-performance-overview)
