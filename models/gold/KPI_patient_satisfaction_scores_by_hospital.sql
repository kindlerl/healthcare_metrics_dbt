{{ 
    config(
        database='HEALTHCARE_DB',
        schema='GOLD',
        materialized='table',
        tags=['gold', 'healthcare']
    )
}}
-- KPI: Patient satisfaction by hospital (if data available)

-- There is no direct patient satisfaction survey data, so we'll use the
-- ratings available in our dim_provider_ratings table, focusing on 3
-- specific ratings:
--   • overall_rating
--   • short_stay_qm_rating
--   • long_stay_qm_rating

-- This KPI is more of a summary table rather than a trending table.
-- Create a summary table with the following columns:
-- +-------------+---------------+-------+----------------+-------------------+------------------+
-- | Provider ID | Provider Name | State | Overall Rating | Short-Stay Rating | Long-Stay Rating |
-- +-------------+---------------+-------+----------------+-------------------+------------------+

-- Filter out rows where all 3 rating columns are null to limit clutter.

SELECT
    dpr.provider_id,
    dp.provider_name,
    dp.state,
    dpr.overall_rating,
    dpr.short_stay_qm_rating AS short_stay_rating,
    dpr.long_stay_qm_rating AS long_stay_rating
FROM 
    HEALTHCARE_DB.SILVER.DIM_PROVIDER_RATINGS dpr
JOIN
    HEALTHCARE_DB.SILVER.DIM_PROVIDER dp
ON
    dpr.provider_id = dp.provider_id
WHERE
    dpr.overall_rating IS NOT NULL
    OR dpr.short_stay_qm_rating IS NOT NULL
    OR dpr.long_stay_qm_rating IS NOT NULL

ORDER BY
    dpr.overall_rating DESC NULLS LAST
