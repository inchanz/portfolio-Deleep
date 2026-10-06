# 03_silver_to_gold.py
"""
Step 3: Silver to Gold Modeling & Performance Optimization.
- Dimensional & Fact modeling / analytical rollups.
- Calculates YoY growth metrics, moving averages, and regional rankings.
- Writes to Gold Delta Lake layer.
- Runs `OPTIMIZE` and `Z-ORDER BY` for query performance.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lag, round as spark_round, current_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from quality_gate import QualityGate

def init_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Medallion-SilverToGold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def create_gold_fact_gdp_metrics(silver_df: DataFrame) -> DataFrame:
    """
    Computes business-level economic metrics:
    - Prior Year GDP
    - YoY Growth Rate (%)
    """
    country_window = Window.partitionBy("country_code").orderBy("year")

    gold_df = (
        silver_df
        .withColumn("prev_year_gdp", lag("gdp_value", 1).over(country_window))
        .withColumn(
            "yoy_growth_pct",
            spark_round(
                ((col("gdp_value") - col("prev_year_gdp")) / col("prev_year_gdp")) * 100,
                2
            )
        )
        .withColumn("_gold_created_at", current_timestamp())
    )

    return gold_df

def optimize_gold_table(spark: SparkSession, gold_delta_path: str):
    """
    Optimizes Delta storage layout using compaction and Z-Ordering.
    """
    print(f"Optimizing table at {gold_delta_path} with Z-ORDER...")
    spark.sql(f"OPTIMIZE delta.`{gold_delta_path}` ZORDER BY (country_code, year)")
    print("Table optimization complete.")

def run_gold_pipeline(spark: SparkSession, silver_delta_path: str, gold_delta_path: str):
    print(f"Reading from Silver: {silver_delta_path}")
    silver_df = spark.read.format("delta").load(silver_delta_path)

    # Transform to Gold analytical model
    gold_df = create_gold_fact_gdp_metrics(silver_df)

    # Post-transformation validation
    print("Validating Gold metrics...")
    QualityGate.validate_not_null(gold_df, "country_code")
    QualityGate.validate_not_null(gold_df, "year")
    QualityGate.validate_unique_keys(gold_df, ["country_code", "year"])

    # Overwrite / Merge into Gold table
    print(f"Writing to Gold Delta table: {gold_delta_path}")
    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_delta_path)
    )

    # Optimize storage and indexing
    optimize_gold_table(spark, gold_delta_path)

if __name__ == "__main__":
    spark = init_spark()
    silver_path = "/tmp/delta/silver/economic_data"
    gold_path = "/tmp/delta/gold/fact_annual_gdp_growth"
    run_gold_pipeline(spark, silver_path, gold_path)
