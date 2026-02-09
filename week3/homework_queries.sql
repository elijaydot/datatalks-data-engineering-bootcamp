-- ================================================
-- Week 3 Homework: Question Queries
-- ================================================

-- ================================================
-- QUESTION 1: Total Record Count
-- ================================================
-- What is count of records for the 2024 Yellow Taxi Data?

SELECT COUNT(*) as record_count
FROM `yellow_taxi_data_wk3.yellow_taxi_regular_table`;

-- Answer: [YOUR_ANSWER_Q1]


-- ================================================
-- QUESTION 2: Data Scanning Estimates
-- ================================================
-- Estimated amount of data read for External vs Materialized Table
-- NOTE: Check estimates in BigQuery UI before running

-- External Table
SELECT COUNT(*) 
FROM `yellow_taxi_data_wk3.external_yellow_taxi`;
-- Estimate: [YOUR_ESTIMATE_EXTERNAL]

-- Materialized Table
SELECT COUNT(*) 
FROM `yellow_taxi_data_wk3.yellow_taxi_regular_table`;
-- Estimate: [YOUR_ESTIMATE_MATERIALIZED]

-- Answer: [YOUR_ANSWER_Q2]


-- ================================================
-- QUESTION 3: Columnar Storage
-- ================================================
-- Why are the estimated number of bytes different?
-- NOTE: Check estimates in BigQuery UI before running

-- Query one column
SELECT PULocationID
FROM `yellow_taxi_data_wk3.yellow_taxi_regular_table`;
-- Estimate: [YOUR_ESTIMATE_ONE_COL]

-- Query two columns
SELECT PULocationID, DOLocationID
FROM `yellow_taxi_data_wk3.yellow_taxi_regular_table`;
-- Estimate: [YOUR_ESTIMATE_TWO_COL]

-- Answer: BigQuery is a columnar database, and it only scans 
-- the specific columns requested in the query.


-- ================================================
-- QUESTION 4: Zero Fare Records
-- ================================================
-- How many records have a fare_amount of 0?

SELECT COUNT(*) as zero_fare_count
FROM `yellow_taxi_data_wk3.yellow_taxi_regular_table`
WHERE fare_amount = 0;

-- Answer: [YOUR_ANSWER_Q4]


-- ================================================
-- QUESTION 5: Optimization Strategy
-- ================================================
-- Best strategy to optimize table for queries that:
-- - Always filter based on tpep_dropoff_datetime
-- - Order results by VendorID

-- Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID
-- (Table created in create_tables.sql)


-- ================================================
-- QUESTION 6: Partitioning Benefits
-- ================================================
-- Retrieve distinct VendorIDs between 2024-03-01 and 2024-03-15
-- Compare estimates between non-partitioned and partitioned tables
-- NOTE: Check estimates in BigQuery UI before running

-- Non-partitioned table
SELECT DISTINCT VendorID
FROM `yellow_taxi_data_wk3.yellow_taxi_regular_table`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
-- Estimate: [YOUR_ESTIMATE_NON_PARTITIONED]

-- Partitioned table
SELECT DISTINCT VendorID
FROM `yellow_taxi_data_wk3.yellow_taxi_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
-- Estimate: [YOUR_ESTIMATE_PARTITIONED]

-- Answer: [YOUR_ANSWER_Q6]


-- ================================================
-- QUESTION 7: External Table Storage
-- ================================================
-- Where is the data stored in the External Table?

-- Answer: GCP Bucket (Google Cloud Storage)
-- Explanation: External tables store only metadata in BigQuery.
-- The actual data remains in the GCS bucket.


-- ================================================
-- QUESTION 8: Always Cluster Data?
-- ================================================
-- Is it best practice in BigQuery to always cluster your data?

-- Answer: False
-- Explanation: Clustering is beneficial for large tables (>1 GB) 
-- with predictable query patterns. For small tables or unpredictable 
-- queries, clustering adds overhead without significant benefit.


-- ================================================
-- QUESTION 9: COUNT(*) Optimization
-- ================================================
-- Write a SELECT count(*) query and check estimated bytes
-- NOTE: Check estimate in BigQuery UI before running

SELECT COUNT(*)
FROM `yellow_taxi_data_wk3.yellow_taxi_regular_table`;
-- Estimate: [YOUR_ESTIMATE_COUNT]

-- Answer: 0 MB (0 Bytes)
-- Why: BigQuery stores row counts in table metadata. For COUNT(*) 
-- queries on materialized tables, it retrieves the pre-computed 
-- count without scanning any data.


-- ================================================
-- ADDITIONAL EXPLORATION QUERIES
-- ================================================

-- Compare query performance across table types
SELECT 
  VendorID,
  COUNT(*) as trip_count,
  AVG(fare_amount) as avg_fare
FROM `yellow_taxi_data_wk3.yellow_taxi_regular_table`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-31'
GROUP BY VendorID
ORDER BY VendorID;

-- Same query on partitioned table (should be faster/cheaper)
SELECT 
  VendorID,
  COUNT(*) as trip_count,
  AVG(fare_amount) as avg_fare
FROM `yellow_taxi_data_wk3.yellow_taxi_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-31'
GROUP BY VendorID
ORDER BY VendorID;

-- Analyze partition distribution
SELECT 
  DATE(tpep_dropoff_datetime) as partition_date,
  COUNT(*) as row_count
FROM `yellow_taxi_data_wk3.yellow_taxi_optimized`
GROUP BY partition_date
ORDER BY partition_date;
