import os
import yaml
from pyspark.sql import SparkSession
from logger import get_logger

logger = get_logger("Load")

def load():
    # When running with spark-submit, the path should be relative to the project root
    with open("./config.yaml", "r") as f:
        config = yaml.safe_load(f)

    db_conf = config["database"]
    processed_dir = config["storage"]["processed"]

    spark = SparkSession.builder.appName("BikeWeatherPipeline").getOrCreate()

    # Load the processed Parquet DIRECTORIES
    logger.info("Reading transformed data from processed directory...")
    df_dim_station = spark.read.parquet(os.path.join(processed_dir, "dim_station"))
    df_dim_weather = spark.read.parquet(os.path.join(processed_dir, "dim_weather"))
    df_fact = spark.read.parquet(os.path.join(processed_dir, "fact_bike_weather"))
    logger.info("DataFrames loaded from Parquet files.")
    # Instead of converting to Pandas and using psycopg2, we can use Spark's JDBC capabilities
    # This is more efficient and handles data types like NULLs correctly.
    
    # Define the JDBC connection properties
    jdbc_url = f"jdbc:postgresql://{db_conf['host']}:{db_conf['port']}/{db_conf['dbname']}"
    connection_properties = {
        "user": db_conf["user"],
        "password": db_conf["password"],
        "driver": "org.postgresql.Driver"  # Telling Spark which driver to use
    }
    
    # Write each DataFrame directly to PostgreSQL
    # Using "append" mode is generally safer and better for dimensional data.
    logger.info("Writing fact_bike_weather to PostgreSQL...")
    df_fact.write.jdbc(
        url=jdbc_url,
        table="fact_bike_weather",
        mode="overwrite",
        properties=connection_properties
    )

    logger.info("Writing dim_station to PostgreSQL...")
    df_dim_station.write.jdbc(
        url=jdbc_url,
        table="dim_station",
        mode="overwrite", # Use "overwrite" if and only if you want to truncate the table first
        properties=connection_properties
    )

    logger.info("Writing dim_weather to PostgreSQL...")
    df_dim_weather.write.jdbc(
        url=jdbc_url,
        table="dim_weather",
        mode="overwrite",
        properties=connection_properties
    )

    logger.info("Writing fact_bike_weather to PostgreSQL...")
    df_fact.write.jdbc(
        url=jdbc_url,
        table="fact_bike_weather",
        mode="append",
        properties=connection_properties
    )

    logger.info("Stage 3 Load complete. Data written to Postgres.")
    spark.stop()


if __name__ == "__main__":
    load()