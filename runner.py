# runner.py
"""
Thin, transparent orchestration layer (~35 lines).
Executes SQL scripts step-by-step and enforces Quality Gate assertion.
"""

from pyspark.sql import SparkSession
import sys

def execute_sql_file(spark: SparkSession, filepath: str):
    print(f"Executing: {filepath}")
    with open(filepath, "r") as f:
        statements = f.read().split(";")
        for stmt in statements:
            cleaned = stmt.strip()
            if cleaned:
                spark.sql(cleaned)

def run_quality_gate(spark: SparkSession):
    print("--> Checking Quality Gate (Nulls & Uniqueness)...")
    
    # Check 1: Null Primary Keys
    null_count = spark.sql("""
        SELECT COUNT(*) as cnt 
        FROM bronze_economic_data 
        WHERE country_code IS NULL OR year IS NULL
    """).collect()[0]["cnt"]
    
    if null_count > 0:
        raise ValueError(f"Quality Gate FAILED: {null_count} null primary keys found!")

    # Check 2: Duplicate Composite Keys
    dup_groups = spark.sql("""
        SELECT COUNT(*) as cnt FROM (
            SELECT country_code, year 
            FROM bronze_economic_data 
            GROUP BY country_code, year 
            HAVING COUNT(*) > 1
        )
    """).collect()[0]["cnt"]

    if dup_groups > 0:
        print(f"[Warning] Found {dup_groups} duplicate key groups in raw data. Deduplication will resolve this in Silver.")
    
    print("--> Quality Gate PASSED!")

def main():
    spark = SparkSession.builder.appName("Medallion-SQL-Pipeline").getOrCreate()
    
    try:
        # Step 1: Bronze
        execute_sql_file(spark, "sql/01_bronze.sql")

        # Step 2: Quality Gate Check
        run_quality_gate(spark)

        # Step 3: Silver MERGE INTO
        execute_sql_file(spark, "sql/03_silver_merge.sql")

        # Step 4: Gold Analytics & ZORDER
        execute_sql_file(spark, "sql/04_gold_analytics.sql")

        print("SUCCESS: End-to-end Medallion pipeline completed successfully!")

    except Exception as err:
        print(f"PIPELINE HALTED: {err}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
