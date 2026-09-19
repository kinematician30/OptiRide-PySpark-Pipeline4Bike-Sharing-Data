# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Extract into Bronze
# MAGIC
# MAGIC Same two APIs as your local `extract.py`. Two things change on Databricks:
# MAGIC
# MAGIC * the raw JSON is written to a **volume** instead of `data/raw/`
# MAGIC * each payload is also appended to a **Bronze Delta table**, so it is queryable and
# MAGIC   keeps its history instead of being overwritten on the next run
# MAGIC
# MAGIC Nothing is cleaned here. Bronze is the record of what the API actually returned.

# COMMAND ----------

import json
import os
from datetime import datetime, timezone

import requests
from pyspark.sql import functions as F

CATALOG = "optiride"
RAW_VOLUME = f"/Volumes/{CATALOG}/bronze/raw"

BIKE_API = "https://api.citybik.es/v2/networks/citi-bike-nyc"
WEATHER_API = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=40.7143&longitude=-74.006"
    "&hourly=temperature_2m,precipitation,wind_speed_10m,cloudcover,relativehumidity_2m"
)

run_ts = datetime.now(timezone.utc)
batch_id = run_ts.strftime("%Y%m%dT%H%M%SZ")
print("batch_id:", batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Call the APIs and land the raw files
# MAGIC
# MAGIC A volume path behaves like an ordinary folder, so plain Python `open()` works. The
# MAGIC batch id in the file name is what lets you replay or audit a single run later.

# COMMAND ----------

def fetch(url: str, label: str) -> dict:
    """Call an API and fail loudly rather than writing an empty file."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()
    print(f"{label}: {len(json.dumps(payload)):,} bytes received")
    return payload


bike_payload = fetch(BIKE_API, "bike")
weather_payload = fetch(WEATHER_API, "weather")

for folder in ("bike", "weather"):
    os.makedirs(f"{RAW_VOLUME}/{folder}", exist_ok=True)

bike_path = f"{RAW_VOLUME}/bike/bike_{batch_id}.json"
weather_path = f"{RAW_VOLUME}/weather/weather_{batch_id}.json"

with open(bike_path, "w") as f:
    json.dump(bike_payload, f)

with open(weather_path, "w") as f:
    json.dump(weather_payload, f)

print("written:", bike_path, weather_path, sep="\n  ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Append each payload to a Bronze table
# MAGIC
# MAGIC `multiline` is needed because each file holds one JSON object rather than one object
# MAGIC per line. `mergeSchema` lets the table absorb a new field if an API adds one, which is
# MAGIC the sort of thing that breaks a local script quietly.

# COMMAND ----------

def land_bronze(file_path: str, table: str) -> int:
    df = (
        spark.read.option("multiline", "true").json(file_path)
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingested_at", F.lit(run_ts).cast("timestamp"))
        .withColumn("source_file", F.lit(file_path))
    )
    (
        df.write.mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"{CATALOG}.bronze.{table}")
    )
    return df.count()


print("bike rows:", land_bronze(bike_path, "bike_raw"))
print("weather rows:", land_bronze(weather_path, "weather_raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Hand the batch id to the next task
# MAGIC
# MAGIC When these notebooks run as a Databricks Job, task values pass small pieces of state
# MAGIC between tasks. Running the notebook on its own, the next notebook simply picks the
# MAGIC latest batch, so this line is a convenience rather than a dependency.

# COMMAND ----------

try:
    dbutils.jobs.taskValues.set(key="batch_id", value=batch_id)
except Exception as exc:  # running interactively, not inside a job
    print("not running as a job task:", exc)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT batch_id, ingested_at, count(*) AS payloads
# MAGIC FROM optiride.bronze.bike_raw
# MAGIC GROUP BY batch_id, ingested_at
# MAGIC ORDER BY ingested_at DESC
# MAGIC LIMIT 5;