-- 02_silver_quality_gate.sql
-- Run pre-write Quality Gate checks on Bronze before merging into Silver.
-- Check 1: Count null primary keys (must be 0)
-- Check 2: Count duplicate composite keys (must be 0)

-- Check 1: Null Primary Keys
SELECT 
    COUNT(*) AS null_key_count
FROM bronze_economic_data
WHERE country_code IS NULL OR year IS NULL;

-- Check 2: Duplicate Composite Keys
SELECT 
    country_code, 
    year, 
    COUNT(*) AS record_count
FROM bronze_economic_data
GROUP BY country_code, year
HAVING COUNT(*) > 1;
