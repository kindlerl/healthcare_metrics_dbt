{{ 
    config(
        database='HEALTHCARE_DB',
        schema='GOLD',
        materialized='table',
        tags=['gold', 'healthcare']
    )
}}

-- KPI: Average nurse-to-patient ratio by hospital, state, and department.
--
-- Assuming MDS_CENSUS is capturing the number of residents
-- MDS = Minimum Data Set
-- Census = number of residents/patients
-- HOWEVER, we do NOT have a "body" metric (like nurse_id, staff_roster_id, etc)
-- so we fall back to a standard practice in healthcare analytics
-- when working with staffing data to count HOURS against # of patients
-- to answer the question:
--
-- "How many hours of nurse time did each patient receive on average?"
--
-- This is often called "Nursing Hours Per Resident Per Day" (HPRD)
--
-- So, the Average nursing hourse to patient ratio would be calculates as:
--
-- Sum(TotalNursingHours) / TotalResidents
--
-- IMPORTANT NOTE:  The KPI requests "by department".  We don't have that data
-- so we will just ignore it.

WITH nurse_metrics AS (
    SELECT 
        fpds.provider_id,
        dp.state,
        TO_CHAR(fpds.staffing_date, 'YYYY-MM') AS month,
        SUM(fpds.mds_census) AS total_patient_days,
        SUM (
            COALESCE(fpds.hrs_rn_don, 0) +
            COALESCE(fpds.hrs_rn_admin, 0) +
            COALESCE(fpds.hrs_rn, 0) +
            COALESCE(fpds.hrs_lpn_admin, 0) +
            COALESCE(fpds.hrs_lpn, 0) +
            COALESCE(fpds.hrs_cna, 0) +
            COALESCE(fpds.hrs_nat_rn, 0) +
            COALESCE(fpds.hrs_med_aide, 0)
        ) AS total_nurse_hours
    FROM 
        HEALTHCARE_DB.SILVER.fact_provider_daily_staffing fpds
    JOIN
        HEALTHCARE_DB.SILVER.dim_provider dp ON fpds.provider_id = dp.provider_id
    GROUP BY
        fpds.provider_id,
        state,
        month
)
SELECT
    dp.provider_name,
    nm.state,
    nm.month,
    nm.total_nurse_hours,
    nm.total_patient_days,
    ROUND((nm.total_nurse_hours / NULLIF(nm.total_patient_days, 0)), 2) AS nurse_hours_to_patient_ratio
FROM
    nurse_metrics nm
JOIN
    HEALTHCARE_DB.SILVER.dim_provider dp ON nm.provider_id = dp.provider_id
ORDER BY
    month,
    state,
    nurse_hours_to_patient_ratio DESC
