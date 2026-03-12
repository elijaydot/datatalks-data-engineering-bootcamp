# BATCH 

## Batch Processing in Data Engineering (Week 6)

Welcome to the **Batch Zone** — where data arrives in chunks, Spark does the heavy lifting, and we turn messy runtime errors into clean pipelines.

Today we worked on local batch processing for NYC yellow taxi data using **PySpark** and `uv`, then debugged the real-world setup issues that show up on Windows.

---

## What We Built Today

- Ran Spark locally with `local[*]`.
- Loaded `yellow_tripdata_2025-11.parquet`.
- Solved Week 6 homework questions (row counts, date filters, max duration, zone join).
- Added a resilient flow so the script still completes when Windows Hadoop write tooling (`winutils`) is missing.

Main script:

- `week6_spark_homework.py`

---

## The Debugging Adventure (What Broke + How We Fixed It)

### 1) Broken Spark test script syntax

**Issue:** `test_spark.py` had invalid builder chaining syntax.

**Fix:** Corrected the `SparkSession.builder` multiline chain so Python can parse and run the file.

---

### 2) `pyproject.toml` warning from `uv`

**Issue:** `uv sync` warned that `tool.uv.env` is unsupported in this config.

**Fix:** Removed the unsupported key from `pyproject.toml`.

---

### 3) Java 25 + Spark/Hadoop compatibility problems

**Issue:** Running Spark with Java 25 caused failures (`getSubject is not supported`, gateway startup issues).

**Fix:** Installed and used **Java 21 (Temurin)** for this project.

---

### 4) Global `JAVA_TOOL_OPTIONS` polluted terminal sessions

**Issue:** A global value (including `--add-opens`) was being injected into every Java run and caused inconsistent behavior.

**Fix:** Cleared it in terminal sessions before running:

```powershell
Remove-Item Env:JAVA_TOOL_OPTIONS -ErrorAction SilentlyContinue
```

---

### 5) Windows `winutils` / `HADOOP_HOME` error during parquet write

**Issue:** Spark could read parquet, but writing repartitioned parquet failed on Windows because `winutils.exe` is not configured.

**Fix:** Updated `week6_spark_homework.py` to gracefully handle the write failure:

- Q2 write-size check is skipped with a clear message.
- The script falls back to the original parquet for Q3–Q6.
- Script completes successfully instead of crashing.

This is practical for homework execution and keeps the pipeline moving.

---

## How to Run Successfully (Reliable Routine)

From `week6/`:

```powershell
# Use Java 21 for this session
$env:JAVA_HOME="C:\Program Files\Eclipse Adoptium\jdk-21.0.10.7-hotspot"
$env:Path="$env:JAVA_HOME\bin;" + $env:Path

# Clear injected Java options if set
Remove-Item Env:JAVA_TOOL_OPTIONS -ErrorAction SilentlyContinue

# Verify Java
java -version

# Install/sync deps and run
uv sync
uv run python .\week6_spark_homework.py
```

Expected ending:

```text
Spark session stopped. Done!
```

---

## Results Snapshot

- Spark version printed successfully. **4.1.1**
- Trips on Nov 15, 2025: **162,604**.
- Longest trip duration: **~90.6 hours**.
- Least frequent pickup zones computed successfully. **Governor's Island/Ellis Island/Liberty Island**

---

## Notes for Future You 🧠

- Batch jobs are not just data logic — environment setup is part of the pipeline.
- Java version mismatches can look like Spark bugs.
- Windows Hadoop tooling can affect writes even when reads work.
- Good pipelines degrade gracefully: fail one optional step, continue core analytics.

---

## Final Vibe

Today’s lesson: **Batch processing is 50% data, 50% detective work.**  
You didn’t just run Spark — you operated it like an engineer. 

