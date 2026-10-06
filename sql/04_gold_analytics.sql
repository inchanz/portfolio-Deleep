-- 04_gold_analytics.sql
-- Create analytical fact table with YoY Growth metric and Optimize storage layout

CREATE TABLE IF NOT EXISTS gold_fact_annual_gdp_growth
USING DELTA
AS
SELECT 
    country_code,
    country_name,
    year,
    gdp_value,
    LAG(gdp_value, 1) OVER (PARTITION BY country_code ORDER BY year) AS prev_year_gdp,
    ROUND(
        ((gdp_value - LAG(gdp_value, 1) OVER (PARTITION BY country_code ORDER BY year)) 
        / LAG(gdp_value, 1) OVER (PARTITION BY country_code ORDER BY year)) * 100, 
        2
    ) AS yoy_growth_pct,
    CURRENT_TIMESTAMP() AS _gold_created_at
FROM silver_economic_data;

-- Delta Storage Performance Optimization:
-- 1. Compaction (merges small files)
-- 2. Multi-dimensional clustering by country and year for data skipping
OPTIMIZE gold_fact_annual_gdp_growth 
ZORDER BY (country_code, year);
