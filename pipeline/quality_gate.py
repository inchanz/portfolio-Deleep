# quality_gate.py
"""
Data Quality Validation Gate.
Performs pre-write and post-write assertions:
- Null rate checks on primary keys / critical columns.
- Duplicate checks on business composite keys.
- Fails pipeline execution early if quality criteria are breached.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when

class DataQualityError(Exception):
    """Raised when data fails quality verification gates."""
    pass

class QualityGate:
    @staticmethod
    def validate_not_null(df: DataFrame, column_name: str, max_null_ratio: float = 0.0) -> None:
        """
        Validates that null percentage in column_name does not exceed max_null_ratio.
        Default is 0.0 (strict NOT NULL for primary keys).
        """
        total_rows = df.count()
        if total_rows == 0:
            raise DataQualityError("Dataset is empty. Quality check halted.")

        null_count = df.filter(col(column_name).isNull()).count()
        null_ratio = null_count / total_rows

        print(f"[QC] Column '{column_name}' null ratio: {null_ratio:.4f} (Threshold: {max_null_ratio})")

        if null_ratio > max_null_ratio:
            raise DataQualityError(
                f"Quality Gate Failed: Column '{column_name}' has {null_count} nulls "
                f"({null_ratio * 100:.2f}%), exceeding tolerance limit of {max_null_ratio * 100:.2f}%."
            )

    @staticmethod
    def validate_unique_keys(df: DataFrame, key_columns: list[str]) -> None:
        """
        Ensures that combination of key_columns has zero duplicates.
        """
        duplicate_count = (
            df.groupBy(key_columns)
            .count()
            .filter(col("count") > 1)
            .count()
        )

        print(f"[QC] Key duplicate instances for {key_columns}: {duplicate_count}")

        if duplicate_count > 0:
            raise DataQualityError(
                f"Quality Gate Failed: Found {duplicate_count} duplicate key groups "
                f"for composite key {key_columns}."
            )

    @staticmethod
    def validate_custom_condition(df: DataFrame, condition_expr: str, error_message: str) -> None:
        """
        Ensures all rows adhere to an arbitrary SQL boolean expression.
        """
        invalid_count = df.filter(f"NOT ({condition_expr})").count()
        if invalid_count > 0:
            raise DataQualityError(
                f"Quality Gate Failed: {invalid_count} records failed condition '{condition_expr}'. "
                f"Reason: {error_message}"
            )
