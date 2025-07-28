{{ 
    config(
        database='HEALTHCARE_DB',
        schema='GOLD',
        materialized='table',
        tags=['gold', 'healthcare']
    )
}}

-- KPI: Comparison of staffing levels vs bed occupancy rates
--
-- Utilize the KPI for Average_nurse_to_patient_ratio and
-- bed_utilization_rates_by_hospital, combining the results to
-- create a direct comparison of the staffing levels and 
-- bed occupancy rate

-- === NURSE HOURS PER PATIENT DATA ===
WITH nurse_metrics AS (
    SELECT 
        provider_id,
        TO_CHAR(staffing_date, 'YYYY-MM') AS month,
        SUM(mds_census) AS total_patient_days,
        SUM (
            COALESCE(hrs_rn_don, 0) +
            COALESCE(hrs_rn_admin, 0) +
            COALESCE(hrs_rn, 0) +
            COALESCE(hrs_lpn_admin, 0) +
            COALESCE(hrs_lpn, 0) +
            COALESCE(hrs_cna, 0) +
            COALESCE(hrs_nat_rn, 0) +
            COALESCE(hrs_med_aide, 0)
        ) AS total_nurse_hours
    FROM 
        HEALTHCARE_DB.SILVER.fact_provider_daily_staffing
    GROUP BY
        provider_id,
        month
),
-- === OCCUPANCY RATE DATA ===
-- Gather all the occupancy rate data.  Do not pull in the state yet
-- since we didn't pull it in with the first CTE.  The provider_id will allow
-- us to pull in the state in the final select
patients_beds AS (
    SELECT 
        fpds.provider_id,
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
        fpds.provider_id,
        month,
        dp.number_of_certified_beds
)
-- === FINAL SELECT ===
SELECT
    dp.provider_name,
    dp.state,
    nm.month,
    ROUND(nm.total_nurse_hours / NULLIF(nm.total_patient_days, 0), 2) AS nurse_hours_to_patient_ratio,
    ROUND(pb.total_patients / NULLIF(pb.number_of_certified_beds, 0) * 100, 2) AS occupancy_rate_pct
FROM
    nurse_metrics nm
JOIN
    patients_beds pb ON nm.provider_id = pb.provider_id
JOIN
    HEALTHCARE_DB.SILVER.dim_provider dp ON nm.provider_id = dp.provider_id
ORDER BY
    month,
    state,
    dp.provider_name
