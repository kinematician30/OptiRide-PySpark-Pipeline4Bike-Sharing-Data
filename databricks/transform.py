# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze to Silver
# MAGIC
# MAGIC This is your `trf.py`, rewritten so the work happens in Spark rather than in a Python
# MAGIC loop. In the local version you looped over `bike_data["network"]["stations"]` and built
# MAGIC a list of dictionaries. Here we `explode` the array inside the DataFrame, which is the
# MAGIC habit that makes the code scale from 2,000 stations to 2,000,000 rows without changing.
# MAGIC
# MAGIC Silver is where cleaning belongs: typing, deduplication, null handling, derived fields.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "optiride"

# take the batch from the job if there is one, otherwise the newest batch in Bronze
try:
    batch_id = dbutils.jobs.taskValues.get(taskKey="extract", key="batch_id")
except Exception:
    batch_id = (
        spark.table(f"{CATALOG}.bronze.bike_raw")
        .select(F.max("batch_id"))
        .collect()[0][0]
    )

print("processing batch:", batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Station readings
# MAGIC
# MAGIC `explode` turns one row holding an array of stations into one row per station. The
# MAGIC `extra` struct differs slightly between CityBikes networks, so `safe` returns a typed
# MAGIC null when a field is missing instead of failing the whole notebook.

# COMMAND ----------

bike_bronze = spark.table(f"{CATALOG}.bronze.bike_raw").filter(F.col("batch_id") == batch_id)

stations = bike_bronze.select(
    F.explode("network.stations").alias("s"),
    "ingested_at",
    "batch_id",
)

available = set(stations.select("s.*").columns)
extra_available = set(stations.select("s.extra.*").columns) if "extra" in available else set()


def safe(path: str, field: str, cast: str):
    """Return the column if the API supplied it, otherwise a typed null."""
    present = field in (extra_available if path == "s.extra" else available)
    column = F.col(f"{path}.{field}") if present else F.lit(None)
    return column.cast(cast)


silver_stations = (
    stations.select(
        safe("s", "id", "string").alias("station_id"),
        safe("s", "name", "string").alias("station_name"),
        safe("s", "latitude", "double").alias("latitude"),
        safe("s", "longitude", "double").alias("longitude"),
        safe("s", "name", "string").alias("address"),
        F.to_timestamp(F.regexp_replace(safe("s", "timestamp", "string"), r"Z$", "")).alias("reading_ts"),
        safe("s", "free_bikes", "int").alias("free_bikes"),
        safe("s", "empty_slots", "int").alias("empty_slots"),
        safe("s.extra", "slots", "int").alias("total_slots"),
        safe("s.extra", "ebikes", "int").alias("ebikes"),
        safe("s.extra", "has_ebikes", "boolean").alias("has_ebikes_flag"),
        F.col("ingested_at"),
        F.col("batch_id"),
    )
    # quality gate: a reading without a station or a time is not usable
    .filter(F.col("station_id").isNotNull() & F.col("reading_ts").isNotNull())
    # derived fields the business asks for
    .withColumn(
        "available_slots",
        F.coalesce(F.col("empty_slots"), F.col("total_slots") - F.col("free_bikes")),
    )
    .withColumn(
        "has_ebikes",
        F.coalesce(F.col("has_ebikes_flag"), F.col("ebikes") > 0, F.lit(False)),
    )
    .withColumn(
        "utilisation_rate",
        F.round(
            F.when(
                F.col("total_slots") > 0,
                F.col("free_bikes") / F.col("total_slots").cast("double"),
            ),
            3,
        ),
    )
    .drop("has_ebikes_flag")
    # one row per station per reading time
    .dropDuplicates(["station_id", "reading_ts"])
)

(
    silver_stations.write.mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.station_status")
)

display(silver_stations.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Weather readings
# MAGIC
# MAGIC Open-Meteo returns parallel arrays: one array of times, one of temperatures, and so
# MAGIC on. `arrays_zip` stitches them into an array of structs, and `explode` gives one row
# MAGIC per hour. That is the Spark way of writing the `zip()` you used locally.

# COMMAND ----------

weather_bronze = spark.table(f"{CATALOG}.bronze.weather_raw").filter(F.col("batch_id") == batch_id)

hourly = weather_bronze.select(
    F.explode(
        F.arrays_zip(
            F.col("hourly.time").alias("time"),
            F.col("hourly.temperature_2m").alias("temperature_2m"),
            F.col("hourly.precipitation").alias("precipitation"),
            F.col("hourly.wind_speed_10m").alias("wind_speed_10m"),
            F.col("hourly.cloudcover").alias("cloudcover"),
            F.col("hourly.relativehumidity_2m").alias("relativehumidity_2m"),
        )
    ).alias("h"),
    "ingested_at",
    "batch_id",
)

silver_weather = (
    hourly.select(
        F.to_timestamp("h.time").alias("weather_ts"),
        F.col("h.temperature_2m").cast("double").alias("temperature_c"),
        F.col("h.precipitation").cast("double").alias("precipitation_mm"),
        F.col("h.wind_speed_10m").cast("double").alias("wind_speed_kmh"),
        F.col("h.cloudcover").cast("double").alias("cloud_cover"),
        F.col("h.relativehumidity_2m").cast("double").alias("humidity"),
        F.col("ingested_at"),
        F.col("batch_id"),
    )
    .filter(F.col("weather_ts").isNotNull())
    .dropDuplicates(["weather_ts"])
)

(
    silver_weather.write.mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.weather_hourly")
)

display(silver_weather.orderBy("weather_ts").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sanity checks before Gold
# MAGIC
# MAGIC Get into the habit of asserting what you believe about the data. A pipeline that fails
# MAGIC here is far cheaper than a dashboard that is quietly wrong for a week.

# COMMAND ----------

checks = (
    spark.table(f"{CATALOG}.silver.station_status")
    .filter(F.col("batch_id") == batch_id)
    .agg(
        F.count("*").alias("rows"),
        F.countDistinct("station_id").alias("stations"),
        F.sum(F.when(F.col("free_bikes") < 0, 1).otherwise(0)).alias("negative_bikes"),
        F.sum(F.when(F.col("total_slots").isNull(), 1).otherwise(0)).alias("missing_slots"),
    )
    .collect()[0]
)

print(checks.asDict())
assert checks["rows"] > 0, "no station rows landed in Silver for this batch"
assert checks["negative_bikes"] == 0, "negative bike counts found, investigate before Gold"