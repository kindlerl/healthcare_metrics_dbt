{{ 
    config(
        database='HEALTHCARE_DB',
        schema='GOLD',
        materialized='table',
        tags=['gold', 'healthcare']
    )
}}

-- KPI: Hospital Occupancy Rate Trends Over the Past Year (Monthly/Quarterly)
--
-- Definition for occupancy rate:
--
--      Occupancy_Rate = (Total_Patients / Total_Beds_Available) * 100
--
-- Total_Patients = mds_census (fact_provider_daily_staffing)
-- Total_Beds_Avaiable = number_of_certified_beds (dim_provider)
-- 
-- Computed by hospital, state, and month.
--
-- HOWEVER, based on current data availability, the staffing fact table
-- (fact_provider_daily_staffing) only provides data for a single date
-- (1970-08-23), so we cannot produce any meaningful montyly or quarterly
-- data.  The current output only reflects occupancy for that specific date.

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