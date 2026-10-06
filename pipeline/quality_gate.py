# quality_gate.py
"""
Data Quality Validation Gate.
Performs essential pre-write sanity checks:
1. Null rate check on primary keys (country_code, year).
2. Unique composite key check.

If either check fails, it raises DataQualityError and halts the pipeline.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class DataQualityError(Exception):
    """Raised when data fails quality verification gates."""
    pass

class QualityGate:
    @staticmethod
    def validate_not_null(df: DataFrame, column_name: str, max_null_ratio: float = 0.0) -> None:
        """
        Ensures null percentage in column_name does not exceed max_null_ratio.
        Default 0.0 means ZERO nulls are allowed for primary keys.
        """
        total_rows = df.count()
        if total_rows == 0:
            raise DataQualityError("Dataset is empty. Quality check halted.")

        null_count = df.filter(col(column_name).isNull()).count()
        null_ratio = null_count / total_rows

        print(f"[QC] Column '{column_name}' null ratio: {null_ratio:.4f} (Allowed: {max_null_ratio})")

        if null_ratio > max_null_ratio:
            raise DataQualityError(
                f"Quality Gate Failed: Column '{column_name}' has {null_count} nulls "
                f"({null_ratio * 100:.2f}%), exceeding limit of {max_null_ratio * 100:.2f}%."
            )

    @staticmethod
    def validate_unique_keys(df: DataFrame, key_columns: list[str]) -> None:
        """
        Ensures that the combination of key_columns has zero duplicates.
        """
        duplicate_count = (
            df.groupBy(key_columns)
            .count()
            .filter(col("count") > 1)
            .count()
        )

        print(f"[QC] Duplicate key groups for {key_columns}: {duplicate_count}")

        if duplicate_count > 0:
            raise DataQualityError(
                f"Quality Gate Failed: Found {duplicate_count} duplicate key groups "
                f"for composite key {key_columns}."
            )
