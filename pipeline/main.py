import sys
import os

from extract import extract
from load import load
from transform import transform
from logger import get_logger

os.environ['PYSPARK_PYTHON'] = r"C:\Users\ojoay\Documents\thePipelineDemo\.venv\Scripts\python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = r"C:\Users\ojoay\Documents\thePipelineDemo\.venv\Scripts\python.exe"

logger = get_logger("Pipeline")


def run_pipeline():
    try:
        logger.info("▶ Stage 1: Extracting data")
        extract()

        logger.info("▶ Stage 2: Transforming data")
        transform()

        logger.info("▶ Stage 3: Loading data into Postgres")
        load()

        logger.info("Pipeline completed successfully!")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
