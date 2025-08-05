{{ 
    config(
        database='HEALTHCARE_DB',
        schema='GOLD',
        materialized='table',
        tags=['gold', 'healthcare']
    )
}}

-- KPI: Bed utilization rates by hospital and department
--
-- This KPI is intended to show bed utilization by hospital and department.
-- Bed utilization is calculated as:
--    (Total Patients / Certified Beds) * 100
--
-- HOWEVER, the dataset does not contain any department-level granularity.
-- Therefore, this KPI can only be calculated at the hospital level.
-- The current output reflects bed utilization per facility.
-- Department-level breakdown is unavailable in the current data.

WITH patients_beds AS (
    SELECT 
        fpds.provider_id,
        dp.provider_name,
        dp.state,
        TO_CHAR(fpds.staffing_date, 'YYYY-MM') AS month,
    
        -- Total number of certified beds
        dp.number_of_certified_beds,

        -- Number of patients
        SUM(fpds.mds_census) AS total_patients,
    
    FROM
        HEALTHCARE_DB.SILVER.fact_provider_daily_staffing fpds
    JOIN
        HEALTHCARE_DB.SILVER.dim_provider dp ON fpds.provider_id = dp.provider_id
    GROUP BY
        1, 2, 3, 4, 5
)
SELECT
    provider_id,
    provider_name,
    state,
    month,
    total_patients,
    number_of_certified_beds,
    ROUND(total_patients / NULLIF(number_of_certified_beds, 0) * 100, 2) AS occupancy_rate_pct
FROM
    patients_beds
ORDER BY
    1, 2, 5 DESC