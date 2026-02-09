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
| **Week 4** | Analytics Engineering | ⏳ Pending | - |
| **Week 5** | Batch Processing | ⏳ Pending | - |
| **Week 6** | Stream Processing | ⏳ Pending | - |
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

---

## Tech Stack

- **Languages**: Python, SQL
- **Containerization**: Docker, Docker Compose
- **Infrastructure**: Terraform
- **Cloud**: Google Cloud Platform (GCP)
- **Database**: PostgreSQL, BigQuery
