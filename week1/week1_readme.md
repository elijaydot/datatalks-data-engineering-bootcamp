# Week 1: Data Engineering Fundamentals

> **NYC Taxi Data Analysis | Docker & PostgreSQL | Terraform & GCP**

A hands-on exploration of data engineering essentials using NYC Green Taxi trip data from November 2025.

---

## Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Setup & Prerequisites](#setup--prerequisites)
- [Questions & Solutions](#questions--solutions)
- [Project Structure](#project-structure)
- [Key Learnings](#key-learnings)

---

## Overview

This project covers the foundational concepts of modern data engineering:
- Containerization with **Docker**
- Database management with **PostgreSQL**
- Data analysis using **SQL**
- Infrastructure as Code with **Terraform**
- Cloud resources on **Google Cloud Platform**

**Dataset**: NYC TLC Green Taxi Trip Records (November 2025)
- 82,548+ trip records
- 265 taxi zones across NYC boroughs

---

## Tech Stack

| Technology | Purpose |
|------------|---------|
| **Docker** | Containerization & environment management |
| **PostgreSQL** | Relational database for data storage |
| **pgAdmin** | Database administration interface |
| **Python** | Data loading and transformation |
| **Terraform** | Infrastructure provisioning |
| **GCP** | Cloud storage (GCS) & data warehouse (BigQuery) |

---

## Setup & Prerequisites

### 1. Docker Environment Setup

```bash
# Start PostgreSQL and pgAdmin
docker-compose up -d

# Verify containers are running
docker ps
```

### 2. Data Acquisition

```bash
# Download NYC Green Taxi data (November 2025)
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet

# Download taxi zone lookup table
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

### 3. Load Data into PostgreSQL

```bash
# Install required Python packages
pip install pandas pyarrow sqlalchemy psycopg2-binary

# Run the data loader (loads in chunks for efficiency)
python load_data.py
```

**Connection Details:**
- **Host**: `localhost` (from host machine) or `db`/`postgres` (from Docker network)
- **Port**: `5433` (external) / `5432` (internal)
- **Database**: `ny_taxi`
- **User/Password**: `postgres`/`postgres`

---

## Questions & Solutions

### Question 1: Understanding Docker Images
**Task**: Run Python 3.13 Docker image and check pip version

```bash
docker run -it --entrypoint bash python:3.13
pip --version
```

**Answer**: `25.3`

**Key Concept**: Docker entrypoints allow you to override the default command and interact with containers directly.

---

### Question 2: Docker Networking
**Task**: Determine the correct hostname and port for pgAdmin to connect to PostgreSQL

**Answer**: 
- **Hostname**: `db` (service name from docker-compose)
- **Port**: `5432` (internal container port, NOT 5433)

**Key Concept**: Services in the same docker-compose network communicate using service names and internal ports, not host-mapped ports.

---

### Question 3: Counting Short Trips
**Task**: Count trips with distance ≤ 1 mile in November 2025

```sql
SELECT COUNT(*) AS trip_count
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1.0;
```

**Key Concept**: Date filtering with inclusive/exclusive bounds ensures accurate temporal queries.

---

### Question 4: Longest Trip by Day
**Task**: Find the pickup day with the longest single trip (excluding trips ≥ 100 miles)

```sql
SELECT 
    DATE(lpep_pickup_datetime) AS pickup_date,
    MAX(trip_distance) AS max_distance
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_distance DESC
LIMIT 1;
```

**Key Concept**: Data quality filters (trip_distance < 100) help remove outliers and errors from analysis.

---

### Question 5: Highest Revenue Pickup Zone
**Task**: Find the pickup zone with the largest total_amount on November 18, 2025

```sql
SELECT 
    tz.Zone AS pickup_zone,
    SUM(gt.total_amount) AS total_amount_sum,
    COUNT(*) AS trip_count
FROM green_taxi_trips gt
JOIN taxi_zones tz ON gt.PULocationID = tz.LocationID
WHERE DATE(gt.lpep_pickup_datetime) = '2025-11-18'
GROUP BY tz.Zone
ORDER BY total_amount_sum DESC
LIMIT 1;
```

**Key Concept**: JOINs allow us to enrich trip data with human-readable zone names instead of cryptic location IDs.

---

### Question 6: Largest Tip by Dropoff Zone
**Task**: Find the dropoff zone with the largest single tip from "East Harlem North" pickups

```sql
SELECT 
    tz_dropoff.Zone AS dropoff_zone,
    gt.tip_amount AS tip
FROM green_taxi_trips gt
JOIN taxi_zones tz_pickup ON gt.PULocationID = tz_pickup.LocationID
JOIN taxi_zones tz_dropoff ON gt.DOLocationID = tz_dropoff.LocationID
WHERE tz_pickup.Zone = 'East Harlem North'
  AND gt.lpep_pickup_datetime >= '2025-11-01'
  AND gt.lpep_pickup_datetime < '2025-12-01'
ORDER BY gt.tip_amount DESC
LIMIT 1;
```

**Key Concept**: Multiple JOINs (self-joins on the same table) enable complex relational queries linking pickup and dropoff locations.

---

### Question 7: Terraform Workflow
**Task**: Identify the correct command sequence for Terraform operations

**Answer**: `terraform init`, `terraform apply -auto-approve`, `terraform destroy`

| Command | Purpose |
|---------|---------|
| `terraform init` | Download providers, set up backend |
| `terraform apply -auto-approve` | Create/update resources without confirmation |
| `terraform destroy` | Remove all managed infrastructure |

**Key Concept**: Terraform enables Infrastructure as Code (IaC), making cloud resources reproducible and version-controlled.

---

## Project Structure

```
Week1/
├── README.md                          # This file
├── docker-compose.yaml                # Docker services definition
├── .gitignore                         # Exclude sensitive files
│
├── data/
│   ├── green_tripdata_2025-11.parquet # NYC taxi trip data
│   └── taxi_zone_lookup.csv           # Zone reference data
│
├── scripts/
│   └── load_data.py                   # Data loading script (chunked)
│
├── queries/
│   ├── q3_short_trips.sql
│   ├── q4_longest_trip.sql
│   ├── q5_top_pickup_zone.sql
│   └── q6_largest_tip.sql
│
└── terraform/
    ├── main.tf                        # GCP resources definition
    ├── variables.tf                   # Variable declarations
    ├── terraform.tfvars               # Variable values
    └── gcp-credentials.json           # Service account key (gitignored)
```

---

## Key Learnings

### Docker & Containerization
✅ Understanding container networking and service communication  
✅ Using docker-compose for multi-container orchestration  
✅ Distinguishing between host ports and container ports  

### SQL & Database Design
✅ Writing efficient queries with proper filtering and indexing  
✅ Performing complex JOINs across multiple tables  
✅ Using aggregation functions (SUM, COUNT, MAX) with GROUP BY  
✅ Implementing data quality checks and outlier removal  

### Data Engineering Best Practices
✅ Loading large datasets in chunks to manage memory  
✅ Implementing proper error handling and progress tracking  
✅ Using connection pooling and batch operations  

### Infrastructure as Code
✅ Provisioning cloud resources with Terraform  
✅ Managing state and applying infrastructure changes  
✅ Following security best practices (service accounts, IAM roles)  

---

## Resources

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Docker Documentation](https://docs.docker.com/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Terraform GCP Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)

---

## Notes

- All SQL queries are optimized for readability and performance
- Data loading uses chunked processing for scalability
- Terraform configuration follows GCP best practices
- Environment variables and credentials are properly secured

---

*Week 1 of Data Engineering Zoomcamp | NYC Taxi Data Analysis*