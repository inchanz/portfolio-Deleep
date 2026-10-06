# Azure Databricks Medallion Lakehouse Pipeline
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=apache-spark&logoColor=white)](https://delta.io/)
[![Azure Databricks](https://img.shields.io/badge/Azure_Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://azure.microsoft.com/en-us/products/databricks/)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Azure ADLS Gen2](https://img.shields.io/badge/Azure_ADLS_Gen2-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/en-us/products/storage/data-lake-storage/)

A production-grade, end-to-end **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** ETL/ELT pipeline implemented with **PySpark** and **Delta Lake** on **Azure Databricks** with **Azure Data Lake Storage Gen2 (ADLS Gen2)**.

---

## 📌 Key Architectural Highlights
- **Medallion Architecture**: Segregated storage into Landing, Bronze (Raw append-only), Silver (Enriched & Deduplicated), and Gold (Analytical Fact & Aggregate).
- **Data Quality Circuit Breaker**: Automated pre-write assertions (null rate thresholds, composite key uniqueness checks, calendar boundary validations) that halt pipeline execution upon quality breach.
- **Idempotent Upserts**: Zero duplicate loads handled via Delta Lake `MERGE INTO` operations.
- **Engineered Audit Trail**: Automatic lineage tracking via metadata attributes (`_ingestion_timestamp`, `_source_file_name`, `_batch_id`, `_silver_updated_at`).
- **Lakehouse Storage Optimization**: File compaction and multi-dimensional clustering using `OPTIMIZE` and `Z-ORDER BY (country_code, year)`.

---

## 🏛️ Architecture Flow

```mermaid
flowchart LR
    subgraph Ingestion["Ingestion Tier"]
        A["External Feed / REST API"] -->|"Raw Ingestion"| L[("ADLS Gen2 Landing")]
    end

    subgraph Medallion["Delta Lake Medallion Tiers"]
        L -->|"Preserve Raw + Audit Columns"| B[("Bronze Delta Table\n(Raw Append-Only)")]
        B -->|"Data Quality Gate\n+ Deduplication"| DQ{"Quality Gate\nAssertions"}
        DQ -->|"Pass: MERGE INTO"| S[("Silver Delta Table\n(Cleansed & Conformed)")]
        DQ -.->|"Fail: Alert & Halt"| DLQ["Pipeline Failure / DLQ"]
        S -->|"Lag Metrics\n+ Fact Modeling"| G[("Gold Delta Table\n(Aggregated Facts)")]
    end

    subgraph Serving["Analytics & Consumption Tier"]
        G -->|"Z-ORDER Optimized"| BI["Power BI / Synapse Analytics / SQL"]
    end

    style DQ fill:#f9f,stroke:#333,stroke-width:2px
    style DLQ fill:#fdd,stroke:#c00,stroke-width:1px
    style G fill:#dfd,stroke:#090,stroke-width:2px
```

---

## 📂 Repository Structure

```
├── .github/
│   └── workflows/
│       └── ci-pipeline.yml         # CI workflow running automated unit & quality tests
├── pipeline/
│   ├── config.py                   # ADLS Gen2 paths, ABFSS URIs & pipeline constants
│   ├── quality_gate.py             # Reusable Data Quality Gate assertion framework
│   ├── 01_landing_to_bronze.py     # Schema-on-read ingestion with audit tracking
│   ├── 02_bronze_to_silver.py      # Cleansing, window deduplication & MERGE upsert
│   ├── 03_silver_to_gold.py        # Analytical modeling & Delta Z-ORDER optimization
│   ├── orchestrator.py             # End-to-end execution runner
│   └── README.md                   # Technical setup instructions
└── tests/
    └── test_quality_gate.py        # Unit tests for Quality Gate assertions
```

---

## 🛠️ Step-by-Step Data Flow

### 1. Landing to Bronze Layer ([`01_landing_to_bronze.py`](pipeline/01_landing_to_bronze.py))
- Reads raw CSV/JSON payload without schema inference (`inferSchema=False`) to avoid ingest drops.
- Enriches records with lineage audit metadata:
  ```python
  bronze_df = (
      raw_df
      .withColumn("_ingestion_timestamp", current_timestamp())
      .withColumn("_source_file_name", input_file_name())
      .withColumn("_batch_id", lit(batch_id))
  )
  ```
- Appends into the Delta Lake `bronze` table.

### 2. Data Quality & Silver Cleansing ([`02_bronze_to_silver.py`](pipeline/02_bronze_to_silver.py))
- Normalizes attributes to standard `snake_case` and enforces explicit type casting.
- Window deduplication: ranks micro-batches by `_ingestion_timestamp DESC` over composite keys `(country_code, year)`.
- **Quality Gate Verification**:
  - `validate_not_null`: Zero tolerance for null primary keys.
  - `validate_unique_keys`: Guarantees composite key integrity.
  - `validate_custom_condition`: Business logic boundaries (e.g., $1900 \le \text{year} \le 2100$).
- **Idempotent Upsert (`MERGE INTO`)**:
  ```python
  silver_table.alias("target").merge(
      source=silver_df.alias("source"),
      condition="target.country_code = source.country_code AND target.year = source.year"
  ).whenMatchedUpdate(set={...}).whenNotMatchedInsert(values={...}).execute()
  ```

### 3. Gold Analytical Modeling & Z-Order Indexing ([`03_silver_to_gold.py`](pipeline/03_silver_to_gold.py))
- Computes analytical metrics such as Year-over-Year (YoY) growth percentages using window `lag()`.
- Collocates data and resolves small file fragmentation using Delta Lake's `OPTIMIZE` and `Z-ORDER`:
  ```sql
  OPTIMIZE delta.`<gold_path>` ZORDER BY (country_code, year)
  ```

---

## 🧪 Automated Testing

Unit tests for the Quality Gate assertions run in CI:
```bash
pytest tests/
```

---

## 👤 Author
- **Deleep Augustian** — [LinkedIn](https://www.linkedin.com/in/deleep-augustian-1a91a110/)

