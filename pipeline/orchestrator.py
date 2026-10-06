# master_pipeline_runner.py
"""
Orchestrator script to execute the end-to-end Medallion pipeline.
Can be executed as a Databricks Job task or Azure Data Factory / Airflow trigger.
"""

from pyspark.sql import SparkSession
import sys
import traceback

import importlib
config = importlib.import_module("config")
landing_to_bronze = importlib.import_module("01_landing_to_bronze")
bronze_to_silver = importlib.import_module("02_bronze_to_silver")
silver_to_gold = importlib.import_module("03_silver_to_gold")


def main():
    print("==================================================")
    print("STARTING MEDALLION ARCHITECTURE PIPELINE EXECUTION")
    print("==================================================")

    spark = (
        SparkSession.builder
        .appName("Medallion-MasterPipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    # Local temp path or ADLS Gen2 path
    landing_file = "/tmp/raw_economic_data.csv"
    bronze_path = "/tmp/delta/bronze/economic_data"
    silver_path = "/tmp/delta/silver/economic_data"
    gold_path = "/tmp/delta/gold/fact_annual_gdp_growth"

    try:
        # Step 1: Landing -> Bronze
        print("\n--- STEP 1: Ingest Landing to Bronze ---")
        landing_to_bronze.ingest_raw_to_landing(config.PipelineConfig.DATASET_URL, landing_file)
        landing_to_bronze.run_bronze_ingestion(spark, landing_file, bronze_path)

        # Step 2: Bronze -> Silver
        print("\n--- STEP 2: Cleansing & Upsert into Silver ---")
        bronze_to_silver.run_silver_pipeline(spark, bronze_path, silver_path)

        # Step 3: Silver -> Gold
        print("\n--- STEP 3: Analytical Modeling & Optimizing Gold ---")
        silver_to_gold.run_gold_pipeline(spark, silver_path, gold_path)

        print("\n==================================================")
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print("==================================================")

    except Exception as e:
        print(f"\n[ERROR] Pipeline failed with error: {str(e)}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
