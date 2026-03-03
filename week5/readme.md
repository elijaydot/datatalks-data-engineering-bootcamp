# Week 5: Declarative Data Pipelines with Bruin & DuckDB

Welcome to Week 5! In this module, we built a complete, modern, and local-first data pipeline for NYC taxi trip data. We moved from theory to practice, using a declarative data build tool, **Bruin**, to orchestrate our pipeline and **DuckDB** as our lightning-fast, file-based data warehouse.

This document explains what we built, the technologies we used, and how it all fits together.

---

## Overview

The goal of this week was to construct a robust, multi-stage ELT (Extract, Load, Transform) pipeline. We fetch raw data from a public source, load it into our local data warehouse, and then run a series of transformations to clean, enrich, and aggregate it into a final, useful report.

Our entire pipeline is defined as a collection of "assets," where each asset represents a table or view in our database. This asset-based approach, powered by Bruin, makes our pipeline modular, easy to understand, and simple to maintain.

---

## Core Technologies

-   **Bruin**: The star of the show. Bruin is a declarative data build tool that allows us to define our pipeline as a graph of assets. We define each asset's properties—its name, type, dependencies, and materialization strategy—directly inside our SQL and Python files using a simple `@bruin` header. Bruin then intelligently builds the dependency graph and executes the assets in the correct order.

-   **DuckDB**: Our choice for a zero-setup, high-performance analytical database. All our data is stored in a single `duckdb.db` file within the project. This makes the entire pipeline incredibly portable and eliminates the need for complex database server management.

---

## Our Data Pipeline in Three Acts

We structured our pipeline into three logical layers: ingestion, staging, and reporting. This is a common best practice that promotes modularity and reusability.

### Act I: Ingestion (Getting the Raw Data)

The first step is to get data into our DuckDB warehouse.

-   `ingestion.trips` (Python): This powerful Python asset is responsible for fetching the raw taxi trip data. It dynamically constructs URLs for the required months, downloads the Parquet files, standardizes column names between different taxi types (e.g., yellow vs. green), and loads the data into the `ingestion.trips` table.
-   `ingestion.payment_lookup` (Seed): A simple but crucial `duckdb.seed` asset. It loads the static `payment_lookup.csv` file into a table, giving us a way to map numeric payment type IDs to human-readable names.

### Act II: Staging (Cleaning and Preparing)

Once the raw data is loaded, we need to clean it up and prepare it for analysis.

-   `staging.trips` (SQL): This SQL asset takes the raw trip data, joins it with our `payment_lookup` table to add the `payment_type_name`, and, most importantly, removes any duplicate records. This creates a clean, reliable, and unique set of trip data that serves as the foundation for all our reporting.

### Act III: Reporting (Aggregating for Insights)

With clean data in our staging layer, we can now create our final report.

-   `reports.trips_report` (SQL): This is the final asset in our pipeline. It reads from the `staging.trips` table and aggregates the data to produce a daily summary. It calculates the total number of trips, total fare amounts, and average fare amounts, grouped by date, taxi type, and payment type.

---

## Configuration is Key

Our pipeline's behavior is controlled by two central configuration files:

-   **.bruin.yml**: This is the main project configuration file. It defines our `default` environment and tells Bruin how to connect to our DuckDB database by specifying the path to the `duckdb.db` file.
-   **pipeline.yml**: This file defines the blueprint for our `nyc_taxi` pipeline. It sets the default schedule, the earliest date for backfills, and defines pipeline-level variables like `taxi_types` and `max_months_per_run`. These variables allow us to change the pipeline's behavior without modifying the code.

---

## How to Run the Pipeline

You can execute the entire pipeline for a specific date range using the Bruin CLI. For example, to run the pipeline for January 2022:

```bash
bruin run ./pipeline/pipeline.yml --start-date 2022-01-01 --end-date 2022-02-01
```

Bruin will automatically validate the assets, build the dependency graph, and run each step in the correct order, applying incremental materialization strategies where defined.

---

## What We Accomplished

-   **Built a Robust Pipeline**: We created a complete, multi-layered data pipeline from scratch.
-   **Mastered Declarative Assets**: We learned how to define data transformations as declarative, self-documenting assets.
-   **Integrated Python and SQL**: We leveraged Python for complex data fetching and SQL for powerful, set-based transformations.
-   **Ensured Data Quality**: We implemented data quality checks (e.g., `not_null`, `unique`) to ensure the reliability of our data.
-   **Embraced a Local-First Workflow**: We built and ran a production-grade pipeline entirely on our local machine, demonstrating a fast and efficient development loop.