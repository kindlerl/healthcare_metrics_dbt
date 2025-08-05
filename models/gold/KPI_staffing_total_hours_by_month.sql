{{ 
    config(
        database='HEALTHCARE_DB',
        schema='GOLD',
        materialized='table',
        tags=['gold', 'healthcare']
    )
}}

-- KPI: Total hours worked by nurses per hospital, state, and month
--

-- === NURSE HOURS  ===
WITH nurse_metrics AS (
    SELECT 
        fpds.provider_id,
        dp.state,
        TO_CHAR(fpds.staffing_date, 'YYYY-MM') AS month,
        SUM (
            COALESCE(fpds.hrs_rn_don, 0) +
            COALESCE(fpds.hrs_rn_admin, 0) +
            COALESCE(fpds.hrs_rn, 0) +
            COALESCE(fpds.hrs_lpn_admin, 0) +
            COALESCE(fpds.hrs_lpn, 0) +
            COALESCE(fpds.hrs_cna, 0) +
            COALESCE(fpds.hrs_nat_rn, 0) +
            COALESCE(fpds.hrs_med_aide, 0)
        ) AS total_hours_worked
    FROM 
        HEALTHCARE_DB.SILVER.fact_provider_daily_staffing fpds
    JOIN
        HEALTHCARE_DB.SILVER.dim_provider dp ON fpds.provider_id = dp.provider_id
    GROUP BY
        fpds.provider_id,
        state,
        month
)
-- === FINAL SELECT, ADD PROVIDER NAME ===
SELECT
    dp.provider_name,
    nm.state,
    nm.month,
    nm.total_hours_worked
FROM
    nurse_metrics nm
JOIN
    HEALTHCARE_DB.SILVER.dim_provider dp ON nm.provider_id = dp.provider_id
ORDER BY
    month,
    state,
    provider_name



