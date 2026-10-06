# config.py
"""
Configuration settings for Azure Databricks Medallion Architecture Pipeline.
Supports Azure Data Lake Storage Gen2 (ADLS Gen2) ABFSS paths or local DBFS/Unity Catalog locations.
"""

from dataclasses import dataclass

@dataclass(frozen=True)
class PipelineConfig:
    # ADLS Gen2 Storage Account Config (Override with your storage details or Azure Key Vault secrets)
    STORAGE_ACCOUNT: str = "dwhstoragegen2"
    CONTAINER_LANDING: str = "landing"
    CONTAINER_BRONZE: str = "bronze"
    CONTAINER_SILVER: str = "silver"
    CONTAINER_GOLD: str = "gold"

    # Base URI (abfss://<container>@<storage_account>.dfs.core.windows.net/ or /mnt/...)
    # Using mount points or direct abfss URIs:
    BASE_STORAGE_URI: str = f"abfss://{{container}}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

    # Dataset details: Stats NZ Geographic / Area Unit or Business Demography open dataset
    DATASET_URL: str = (
        "https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv"  # High-availability benchmark dataset
    )
    # Alternatively, Stats NZ Annual Enterprise Survey or Electronic Card Transactions
    SOURCE_NAME: str = "economic_indicators_feed"

    # Catalog & Database Names (Unity Catalog or Hive Metastore)
    CATALOG_NAME: str = "portfolio_catalog"
    SCHEMA_NAME: str = "economic_data"

    # Paths for Delta storage
    @classmethod
    def get_path(cls, layer: str, table_name: str) -> str:
        return f"{cls.BASE_STORAGE_URI.format(container=layer)}/{table_name}"
