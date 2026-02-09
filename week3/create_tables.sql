-- ================================================
-- Week 3 Homework: BigQuery Table Creation
-- ================================================

-- Replace [YOUR_BUCKET_NAME] with your actual GCS bucket name
-- Replace [YOUR_PROJECT_ID] with your actual project ID

-- ================================================
-- 1. CREATE DATASET
-- ================================================
-- Note: This is typically done via console or gcloud CLI
-- CREATE SCHEMA IF NOT EXISTS yellow_taxi_data_wk3;

-- ================================================
-- 2. CREATE EXTERNAL TABLE
-- ================================================
-- External table pointing to parquet files in GCS
-- Data remains in Cloud Storage

CREATE OR REPLACE EXTERNAL TABLE `yellow_taxi_data_wk3.external_yellow_taxi`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://[YOUR_BUCKET_NAME]/yellow_tripdata_2024-*.parquet']
);

-- ================================================
-- 3. CREATE MATERIALIZED (REGULAR) TABLE
-- ================================================
-- Copies data from external table into BigQuery storage
-- Non-partitioned, non-clustered

CREATE OR REPLACE TABLE `yellow_taxi_data_wk3.yellow_taxi_regular_table` AS
SELECT * FROM `yellow_taxi_data_wk3.external_yellow_taxi`;

-- ================================================
-- 4. CREATE OPTIMIZED TABLE
-- ================================================
-- Partitioned by dropoff datetime
-- Clustered by VendorID

CREATE OR REPLACE TABLE `yellow_taxi_data_wk3.yellow_taxi_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `yellow_taxi_data_wk3.yellow_taxi_regular_table`;

-- ================================================
-- VERIFICATION QUERIES
-- ================================================

-- Check row counts
SELECT 
  COUNT(*) as external_count 
FROM `yellow_taxi_data_wk3.external_yellow_taxi`;

SELECT 
  COUNT(*) as regular_count 
FROM `yellow_taxi_data_wk3.yellow_taxi_regular_table`;

SELECT 
  COUNT(*) as optimized_count 
FROM `yellow_taxi_data_wk3.yellow_taxi_optimized`;

-- Preview data
SELECT * 
FROM `yellow_taxi_data_wk3.yellow_taxi_regular_table` 
LIMIT 10;

-- Check table info
SELECT 
  table_name,
  ROUND(size_bytes/1024/1024/1024, 2) as size_gb,
  row_count
FROM `yellow_taxi_data_wk3.__TABLES__`;
