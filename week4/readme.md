# Week 4: Analytics Engineering (dbt)

This week focused on building analytics models with **dbt** on BigQuery using NYC taxi data.

## Context and approach

- Started with local experimentation and then moved the main modeling workflow to **Google BigQuery**.
- The move helped reduce local machine pressure (storage and processing limits on a 16GB laptop) while keeping transformations reproducible in dbt.
- BigQuery became the primary warehouse for model execution and validation.

## What was implemented

- Set up and ran the dbt project in `week4/taxi_rides_ny`.
- Built staging models:
  - `stg_green_tripdata`
  - `stg_yellow_tripdata`
  - `stg_fhv_tripdata`
- Built core models:
  - `fct_trips` (view)
  - `dim_zones` (table)
  - `fct_monthly_zone_revenue` (table)
- Loaded seed data:
  - `taxi_zone_lookup`

## Issues fixed during execution

While running models on BigQuery, a few SQL compatibility issues were resolved:

- Replaced unsupported cast type `varchar` with `string`.
- Updated month truncation syntax to BigQuery format (`date_trunc(date(...), month)`).
- Aligned `UNION ALL` column data types in `fct_trips` using explicit casts for nullable columns.

After these fixes, the targeted dbt build completed successfully.

## Challenges encountered

- Local environment constraints: running larger transformations and artifacts locally can consume significant disk and memory.
- Warehouse SQL differences: some SQL syntax that works in other engines required adaptation for BigQuery.
- Package/artifact handling on Windows: local folder locks required careful cleanup and reruns.

These were handled by shifting execution to BigQuery and fixing model SQL to be BigQuery-compatible.

## Why dbt was important

- It provided a clear modeling structure (staging → core) instead of ad-hoc SQL scripts.
- It made dependencies explicit, so lineage and build order were easy to reason about.
- It improved reliability through repeatable builds, seeds, and tests.
- It made debugging easier because failures pointed to specific models and compiled SQL.

## Lessons learned

- Keep transformations close to the warehouse for better scalability and fewer local resource issues.
- Be explicit with data types when unioning datasets from different sources.
- Validate SQL functions and type names against the target warehouse (BigQuery in this case).
- Use dbt selectors (`+model`) to include dependencies during targeted runs.
- Maintain clean project hygiene (ignore generated artifacts and credentials in git).

## Useful commands

From `week4/taxi_rides_ny`:

- Install deps: `uv run dbt deps`
- Build selected models with dependencies: `uv run dbt build --select +fct_trips +fct_monthly_zone_revenue`
- Build full project: `uv run dbt build`
- Run tests only: `uv run dbt test`
- Generate docs: `uv run dbt docs generate`
- Serve docs locally: `uv run dbt docs serve --port 8080`

## Notes

- Some runs showed Google ADC warnings about missing quota project. These warnings did not block model execution.
- Models are visible in BigQuery under the target dataset (for example `dev` or `prod`, depending on your profile target).
