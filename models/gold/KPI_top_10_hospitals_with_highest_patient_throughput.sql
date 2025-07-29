{{ 
    config(
        database='HEALTHCARE_DB',
        schema='GOLD',
        materialized='table',
        tags=['gold', 'healthcare']
    )
}}


-- KPI: Top 10 Hospitals With The Highest Patient Throughput

-- NOTE: Throughput requires a date range to track patient movement over time.
-- Since the available data only includes a single day (1970-08-23),
-- this metric cannot be accurately computed as intended.

-- The query below represents a SNAPSHOT of the top 10 facilities with the
-- highest patient count on that single day

SELECT
    dp.provider_id,
    dp.provider_name,
    dp.state,
    SUM(fpds.mds_census) AS patient_count
FROM
    HEALTHCARE_DB.SILVER.dim_provider dp
JOIN
    HEALTHCARE_DB.SILVER.fact_provider_daily_staffing fpds ON dp.provider_id = fpds.provider_id
GROUP BY
    dp.provider_id, dp.provider_name, dp.state
ORDER BY
    patient_count DESC
LIMIT
    10
    