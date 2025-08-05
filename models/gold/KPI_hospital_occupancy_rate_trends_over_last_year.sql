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
--      Occupancy_Rate = (Total_Patients_for_Period) / (Total_Beds_Available * Number_Days_in_Period)) * 100
--
-- Total_Patients_for_Period = Sum(mds_census) across period of time (fact_provider_daily_staffing)
-- Total_Beds_Avaiable = (number_of_certified_beds * number_of_days_in_period) (dim_provider)
-- 
-- Computed by hospital, state, and month.
--

WITH patients_beds AS (
    SELECT 
        fpds.provider_id,
        dp.provider_name,
        dp.state,
        DATE_TRUNC('month', fpds.staffing_date) AS month_start_date,
    
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
        dp.provider_name,
        dp.state,
        month_start_date,
        number_of_certified_beds
),
days_in_period AS (
    SELECT
        provider_id,
        provider_name,
        state,
        month_start_date,
        number_of_certified_beds,
        total_patients,

        -- Days in month
        DAY(LAST_DAY(month_start_date)) AS days_in_month,

        -- Quarter (e.g. 2024 Q2)
        TO_CHAR(DATE_TRUNC('quarter', month_start_date), 'YYYY') || ' Q' || EXTRACT(QUARTER FROM month_start_date) AS quarter_label,

        -- Days in quarter
        DATEDIFF(
            day,
            DATE_TRUNC('quarter', month_start_date),
            DATEADD(quarter, 1, DATE_TRUNC('quarter', month_start_date))
        ) AS days_in_quarter
    FROM
        patients_beds
)
SELECT
    provider_id,
    provider_name,
    state,
    month_start_date,
    quarter_label,
    days_in_month,
    days_in_quarter,
    total_patients,
    number_of_certified_beds,
    ROUND((total_patients / (NULLIF(COALESCE(number_of_certified_beds, 0), 0) * days_in_month)) * 100, 2) AS occupancy_rate_pct
FROM
    days_in_period
ORDER BY
    provider_id,
    provider_name,
    month_start_date ASC,
    occupancy_rate_pct DESC