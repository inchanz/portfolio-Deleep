# runner.py
"""
Thin, transparent orchestration layer (~40 lines).
Executes SQL scripts step-by-step and enforces Quality Gate circuit breaker.
"""

from pyspark.sql import SparkSession
import sys

def execute_sql_file(spark: SparkSession, filepath: str):
    """Reads a .sql file, splits on semicolons, and executes statements."""
    print(f"[STAGE] Executing: {filepath}")
    with open(filepath, "r") as f:
        statements = f.read().split(";")
        for stmt in statements:
            cleaned = stmt.strip()
            if cleaned:
                spark.sql(cleaned)

def run_quality_gate(spark: SparkSession):
    """
    Executes pre-write Quality Gate checks on Bronze before Silver write.
    1. Null check on primary keys (MUST be 0).
    2. Duplicate check on composite keys.
    """
    print("[GATE] Checking Quality Gate assertions on Bronze table...")

    # Check 1: Null Primary Keys (Hard Stop)
    null_result = spark.sql("""
        SELECT COUNT(*) AS cnt 
        FROM bronze_economic_data 
        WHERE country_code IS NULL OR year IS NULL
    """).collect()[0]["cnt"]

    if null_result > 0:
        raise ValueError(
            f"Quality Gate FAILED: Found {null_result} null primary keys! "
            f"Halting pipeline before Silver ingestion."
        )
    print(f"  ✓ Check 1 Passed: 0 null primary keys found.")

    # Check 2: Duplicate Composite Keys
    dup_result = spark.sql("""
        SELECT COUNT(*) AS cnt FROM (
            SELECT country_code, year 
            FROM bronze_economic_data 
            GROUP BY country_code, year 
            HAVING COUNT(*) > 1
        )
    """).collect()[0]["cnt"]

    if dup_result > 0:
        print(f"  ! Notice: {dup_result} duplicate key groups detected in Bronze. Deduplication window will resolve this in Silver.")
    else:
        print(f"  ✓ Check 2 Passed: 0 duplicate keys detected.")

    print("[GATE] Quality Gate checks completed successfully.\n")

def main():
    spark = SparkSession.builder.appName("Medallion-SQL-Pipeline").getOrCreate()

    try:
        # Step 1: Bronze Ingestion
        execute_sql_file(spark, "sql/01_bronze.sql")

        # Step 2: Quality Gate (runs strictly BEFORE Silver merge)
        run_quality_gate(spark)

        # Step 3: Silver Cleansing & MERGE INTO Upsert
        execute_sql_file(spark, "sql/03_silver_merge.sql")

        # Step 4: Gold Analytics & ZORDER Compaction
        execute_sql_file(spark, "sql/04_gold_analytics.sql")

        print("SUCCESS: End-to-end Medallion pipeline completed successfully!")

    except Exception as err:
        print(f"[CRITICAL ERROR] Pipeline halted: {err}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
