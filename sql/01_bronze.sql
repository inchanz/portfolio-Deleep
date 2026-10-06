-- 01_bronze.sql
-- Ingest raw records from landing zone into Bronze Delta table
-- Schema-on-read: keep columns as raw strings, append lineage audit metadata

CREATE TABLE IF NOT EXISTS bronze_economic_data (
    country_name STRING,
    country_code STRING,
    year STRING,
    value STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file_name STRING
)
USING DELTA;

-- Append newly arrived raw files into bronze
INSERT INTO bronze_economic_data
SELECT 
    Country_Name AS country_name,
    Country_Code AS country_code,
    Year AS year,
    Value AS value,
    CURRENT_TIMESTAMP() AS _ingestion_timestamp,
    input_file_name() AS _source_file_name
FROM csv.`/tmp/raw_economic_data.csv`;
