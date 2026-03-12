# Data Engineering Zoomcamp Journey

Welcome to my repository for the **Data Engineering Zoomcamp**! This project documents my hands-on journey through modern data engineering practices, tools, and workflows over an 8-week curriculum.

Here you'll find code, notes, and solutions for each module as I progress through the course.

---

## Course Syllabus & Progress

| Week | Topic | Status | Link |
| :--- | :--- | :---: | :---: |
| **Week 1** | **Containerization & Infrastructure as Code** | ✅ Done | [View Folder](./week1/) |
| **Week 2** | **Workflow Orchestration** | ✅ Done | [View Folder](./week2/) |
| **Week 3** | **Data Warehousing & BigQuery** | ✅ Done | [View Folder](./week3/) |
| **Week 4** | **Analytics Engineering (dbt)** | ✅ Done | [View Folder](./week4/) |
| **Week 5** | **Batch Processing (Bruin & DuckDB)** | ✅ Done | [View Folder](./week5/) |
| **Week 6** | **Batch Processing with Spark** | ✅ Done | [View Folder](./week6/) |
| **Week 7** | Project | ⏳ Pending | - |
| **Week 8** | Project | ⏳ Pending | - |

---

## Weekly Highlights

### [Week 1: Containerization & Infrastructure as Code](./week1/)
> **Focus**: Docker, PostgreSQL, SQL, Terraform, GCP

In the first week, we established the foundational infrastructure for data engineering.
- **Containerization**: Used Docker and Docker Compose to run PostgreSQL and pgAdmin.
- **Database**: Ingested NYC Green Taxi data (Nov 2025) into Postgres.
- **SQL Analysis**: Performed complex queries to analyze trip records.
- **IaC**: Provisioned cloud resources (GCS, BigQuery) on Google Cloud Platform using Terraform.

### [Week 2: Workflow Orchestration](./week2/)
> **Focus**: Kestra, Workflow Automation, Development Environment Configuration

Building on the foundational infrastructure, week two focuses on orchestrating data pipelines and automating workflows.
- **Environment Setup**: Optimized development container with Visual Studio Code extensions for seamless collaboration.
- **Security First**: Implemented proper credential management with gitignore for sensitive Google Cloud Platform credentials.
- **Workflow Automation**: Learned to build and deploy data pipelines using Kestra workflow orchestration platform.
- **YAML Workflows**: Defined complex data pipelines through version-controlled YAML configurations.
- **Local Development**: Set up comprehensive local development environment with port forwarding for Kestra, PostgreSQL, and pgAdmin.

### [Week 3: Data Warehousing & BigQuery](./week3/)
> **Focus**: Data Warehousing, BigQuery, Cloud Storage, Table Optimization

Exploring data warehousing concepts using Google BigQuery and Cloud Storage with NYC Yellow Taxi data.
- **Data Ingestion**: Developed Python script to download and upload 6 months of taxi trip data (20M+ records) to Google Cloud Storage in parallel using ThreadPoolExecutor.
- **External Tables**: Created external tables in BigQuery that reference parquet files in GCS without duplicating data.
- **Materialized Tables**: Built materialized tables in BigQuery's native columnar format for improved query performance.
- **Table Optimization**: Implemented partitioning by `tpep_dropoff_datetime` and clustering by `VendorID` to optimize query performance and reduce data scanned.
- **BigQuery Insights**: Learned columnar storage benefits, metadata optimization, and cost-efficient query patterns.
- **Performance Tuning**: Demonstrated dramatic performance improvements through partitioning (reducing data scanned from 310MB to 27MB on date-range queries).

### [Week 4: Analytics Engineering (dbt)](./week4/)
> **Focus**: dbt Modeling, Seeds, Data Testing, BigQuery Compatibility

In week four, I used dbt to structure analytics-ready models on top of taxi datasets.
- **Modeling Layers**: Built staging and core models (`stg_*`, `fct_trips`, `dim_zones`, `fct_monthly_zone_revenue`).
- **Seed Management**: Loaded `taxi_zone_lookup` as seed data for dimensional joins.
- **Execution**: Ran targeted builds and dependency-based runs with dbt selectors.
- **Debugging**: Resolved BigQuery SQL issues involving data types, `date_trunc` syntax, and `UNION ALL` type alignment.
- **Validation**: Confirmed successful model execution after fixes.

### Week 5: Declarative Data Pipelines with Bruin & DuckDB
> **Focus**: Bruin, DuckDB, ELT, Data Quality, Local Development

In week five, we shifted to building a modern, local-first ELT pipeline using Bruin and DuckDB.
- **Declarative Pipelines**: Defined data assets (Python, SQL, Seeds) with embedded configuration (`@bruin` tags) for dependencies and materialization.
- **Ingestion**: Built Python assets to dynamically fetch and standardize NYC Taxi parquet data from public sources.
- **Transformation**: Implemented SQL-based staging and reporting layers with incremental materialization strategies (`time_interval`).
- **Data Quality**: Integrated in-line quality checks (`not_null`, `unique`, `non_negative`) directly within asset definitions.
- **Orchestration**: Managed the full pipeline lifecycle (validate, run, lineage) using the Bruin CLI and local DuckDB storage.

### [Week 6: Batch Processing with Spark](./week6/)
> **Focus**: PySpark, Batch Analytics, Runtime Debugging, Windows Compatibility

In week six, I implemented local batch processing for NYC Yellow Taxi data using Spark and worked through practical environment issues to make the workflow reproducible.
- **Spark Batch Pipeline**: Ran local Spark jobs with `local[*]`, loaded parquet trip data, and completed homework analytics (trip counts, date filters, duration metrics, and zone-based aggregations).
- **Script Reliability**: Fixed syntax issues in `test_spark.py` and improved runtime behavior in `week6_spark_homework.py`.
- **Config Cleanup**: Removed unsupported `uv` config from `week6/pyproject.toml` to eliminate parse warnings.
- **Java Compatibility**: Resolved Java runtime conflicts by standardizing execution on Java 21 instead of Java 25.
- **Terminal Stability**: Diagnosed and neutralized global `JAVA_TOOL_OPTIONS` interference that caused inconsistent Spark startup behavior.
- **Windows Hadoop Edge Case**: Added graceful fallback when parquet write fails due to missing `winutils`/`HADOOP_HOME`, so the script continues and finishes Q3–Q6 successfully.

---

## Tech Stack

- **Languages**: Python, SQL
- **Containerization**: Docker, Docker Compose
- **Infrastructure**: Terraform
- **Cloud**: Google Cloud Platform (GCP)
- **Database**: PostgreSQL, BigQuery, DuckDB
