-- 03_silver_merge.sql
-- Idempotent upsert into Silver Delta table
-- 1. Cleans whitespace, trims strings, casts data types
-- 2. MERGE INTO: Updates matching keys, Inserts new records

CREATE TABLE IF NOT EXISTS silver_economic_data (
    country_code STRING,
    country_name STRING,
    year INT,
    gdp_value DOUBLE,
    _source_file_name STRING,
    _silver_updated_at TIMESTAMP
)
USING DELTA;

-- Staging view with cleaned, typed, and deduplicated data from Bronze
CREATE OR REPLACE TEMP VIEW v_cleaned_bronze AS
SELECT 
    UPPER(TRIM(country_code)) AS country_code,
    TRIM(country_name) AS country_name,
    CAST(year AS INT) AS year,
    CAST(value AS DOUBLE) AS gdp_value,
    _source_file_name,
    CURRENT_TIMESTAMP() AS _silver_updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY country_code, year ORDER BY _ingestion_timestamp DESC) as rn
    FROM bronze_economic_data
    WHERE country_code IS NOT NULL AND year IS NOT NULL
)
WHERE rn = 1;

-- SQL MERGE INTO (familiar T-SQL / ANSI syntax)
MERGE INTO silver_economic_data AS target
USING v_cleaned_bronze AS source
ON target.country_code = source.country_code AND target.year = source.year
WHEN MATCHED THEN
    UPDATE SET 
        target.country_name = source.country_name,
        target.gdp_value = source.gdp_value,
        target._source_file_name = source._source_file_name,
        target._silver_updated_at = source._silver_updated_at
WHEN NOT MATCHED THEN
    INSERT (country_code, country_name, year, gdp_value, _source_file_name, _silver_updated_at)
    VALUES (source.country_code, source.country_name, source.year, source.gdp_value, source._source_file_name, source._silver_updated_at);
