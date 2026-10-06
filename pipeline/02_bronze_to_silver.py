# 02_bronze_to_silver.py
"""
Step 2: Bronze to Silver Transformation & Cleansing.
1. Standardizes column names to snake_case.
2. Cleans whitespace, standardizes uppercase codes, casts data types.
3. Deduplicates incoming rows by (country_code, year).
4. Runs Quality Gate (Not-Null & Uniqueness).
5. Upserts into Silver Delta table using MERGE INTO.
"""

import re
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim, upper, current_timestamp
from delta.tables import DeltaTable

from quality_gate import QualityGate

def init_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Medallion-BronzeToSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def to_snake_case(column_name: str) -> str:
    """Standardizes column names to clean snake_case."""
    cleaned = re.sub(r"[^\w\s]", "", column_name)
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned.lower()

def transform_bronze_to_silver(bronze_df: DataFrame) -> DataFrame:
    """
    Cleanses, types, and dedupes the bronze batch.
    """
    # 1. Standardize column names to snake_case
    renamed_df = bronze_df
    for col_name in bronze_df.columns:
        if not col_name.startswith("_"):
            renamed_df = renamed_df.withColumnRenamed(col_name, to_snake_case(col_name))

    # 2. Type casting and trimming
    cleansed_df = (
        renamed_df
        .withColumn("country_name", trim(col("country_name")))
        .withColumn("country_code", upper(trim(col("country_code"))))
        .withColumn("year", col("year").cast("integer"))
        .withColumn("gdp_value", col("value").cast("double"))
        .filter(col("country_code").isNotNull() & (col("country_code") != ""))
        .filter(col("year").isNotNull())
    )

    # 3. Deduplicate by business key (country_code, year)
    deduped_df = (
        cleansed_df
        .dropDuplicates(["country_code", "year"])
        .withColumn("_silver_updated_at", current_timestamp())
    )

    return deduped_df

def merge_into_silver(spark: SparkSession, silver_df: DataFrame, silver_delta_path: str):
    """
    Upserts into Delta Lake Silver table idempotently using MERGE INTO.
    - If country_code and year already exist: UPDATE the row with new values.
    - If it's a new country_code and year: INSERT the row.
    """
    if not DeltaTable.isDeltaTable(spark, silver_delta_path):
        print(f"Silver table does not exist at {silver_delta_path}. Creating initial table...")
        silver_df.write.format("delta").mode("overwrite").save(silver_delta_path)
        return

    silver_table = DeltaTable.forPath(spark, silver_delta_path)

    (
        silver_table.alias("target")
        .merge(
            source=silver_df.alias("source"),
            condition="target.country_code = source.country_code AND target.year = source.year"
        )
        .whenMatchedUpdate(set={
            "country_name": "source.country_name",
            "gdp_value": "source.gdp_value",
            "_source_file_name": "source._source_file_name",
            "_silver_updated_at": "source._silver_updated_at"
        })
        .whenNotMatchedInsert(values={
            "country_code": "source.country_code",
            "country_name": "source.country_name",
            "year": "source.year",
            "gdp_value": "source.gdp_value",
            "_ingestion_timestamp": "source._ingestion_timestamp",
            "_source_file_name": "source._source_file_name",
            "_silver_updated_at": "source._silver_updated_at"
        })
        .execute()
    )
    print("Silver Delta MERGE completed successfully.")

def run_silver_pipeline(spark: SparkSession, bronze_delta_path: str, silver_delta_path: str):
    print(f"Reading from Bronze: {bronze_delta_path}")
    bronze_df = spark.read.format("delta").load(bronze_delta_path)

    # Transform & Cleanse
    silver_df = transform_bronze_to_silver(bronze_df)

    # --- DATA QUALITY GATES ---
    print("Executing Data Quality Gates on Silver layer...")
    QualityGate.validate_not_null(silver_df, "country_code", max_null_ratio=0.0)
    QualityGate.validate_not_null(silver_df, "year", max_null_ratio=0.0)
    QualityGate.validate_unique_keys(silver_df, ["country_code", "year"])
    print("All Quality Gates Passed!")

    # Merge into Silver
    merge_into_silver(spark, silver_df, silver_delta_path)

if __name__ == "__main__":
    spark = init_spark()
    bronze_path = "/tmp/delta/bronze/economic_data"
    silver_path = "/tmp/delta/silver/economic_data"
    run_silver_pipeline(spark, bronze_path, silver_path)
