# 01_landing_to_bronze.py
"""
Step 1: Ingestion from Landing / Source API/URL to Bronze Layer.
- Downloads / pulls raw payload into landing zone.
- Ingests raw data with full schema preservation.
- Adds metadata/audit columns: `_ingestion_timestamp`, `_source_file_name`.
- Appends into Delta Lake Bronze table.
"""

import urllib.request
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

def init_spark() -> SparkSession:
    return SparkSession.builder.appName("Medallion-LandingToBronze").getOrCreate()

def ingest_raw_to_landing(url: str, landing_landing_path: str):
    """
    Downloads raw payload into ADLS Gen2 landing folder.
    In Databricks, landing_path can be an ADLS mount or /tmp/landing.
    """
    print(f"Downloading dataset from {url} to {landing_landing_path}...")
    local_path = "/tmp/raw_economic_data.csv"
    urllib.request.urlretrieve(url, local_path)
    # In Databricks environment: dbutils.fs.cp(f"file:{local_path}", landing_landing_path)
    return local_path

def run_bronze_ingestion(spark: SparkSession, raw_file_path: str, bronze_delta_path: str):
    """
    Reads raw payload and writes as append-only Delta table with audit columns.
    """
    print(f"Reading raw file from: {raw_file_path}")
    
    # Read raw data without aggressive typing to avoid ingest failures (Schema-on-read)
    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")  # Preserve raw strings to avoid dropping malformed rows
        .csv(raw_file_path)
    )

    # Add audit / lineage metadata columns
    bronze_df = (
        raw_df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file_name", input_file_name())
        .withColumn("_batch_id", lit(datetime.utcnow().strftime("%Y%m%d_%H%M%S")))
    )

    print(f"Writing {bronze_df.count()} records to Bronze Delta table: {bronze_delta_path}")

    # Write as append-only to preserve raw history
    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(bronze_delta_path)
    )

    print("Bronze ingestion completed successfully.")
    return bronze_df

if __name__ == "__main__":
    spark = init_spark()
    
    # Example paths (adjust to your ADLS Gen2 paths or /dbfs/mnt/...)
    sample_url = "https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv"
    landing_file = "/tmp/raw_economic_data.csv"
    bronze_path = "/tmp/delta/bronze/economic_data"
    
    ingest_raw_to_landing(sample_url, landing_file)
    run_bronze_ingestion(spark, landing_file, bronze_path)
