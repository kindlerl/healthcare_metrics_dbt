{{ 
    config(
        database='HEALTHCARE_DB',
        schema='GOLD',
        materialized='table',
        tags=['gold', 'healthcare']
    )
}}

-- KPI: Percentage of nurses working overtime.
--
-- Since we were not provided with an "overtime_hours" metric, we 
-- have to make an approximation using a widely-used healthcare 
-- analytics proxy calculation of:
--
-- Total_Contractor_Hours / Total_Nurse_Hours
--
-- Contractors (_CTR columns) are typically used when staff (_EMP columns)
-- are overextended.  This can be used as a proxy for overtime.
--
-- Percentage Calculation Assumed in Query:  
--      %OvertimeHours = (TotalContractorHours / TotalNurseHours) * 100
--
-- * Be sure to calculate TotalNurseHours as: total(_emp) + total(_ctr)


WITH emp_ctr_total_hours AS (
    SELECT 
        fpds.provider_id,
        dp.provider_name,
        dp.state,
        TO_CHAR(fpds.staffing_date, 'YYYY-MM') AS month,
    
        -- Contractor nurse hours
        SUM(
            COALESCE(fpds.hrs_rn_don_ctr, 0) +
            COALESCE(fpds.hrs_rn_admin_ctr, 0) +
            COALESCE(fpds.hrs_rn_ctr, 0) +
            COALESCE(fpds.hrs_lpn_admin_ctr, 0) +
            COALESCE(fpds.hrs_lpn_ctr, 0) +
            COALESCE(fpds.hrs_cna_ctr, 0) +
            COALESCE(fpds.hrs_nat_rn_ctr, 0) +
            COALESCE(fpds.hrs_med_aide_ctr, 0)
        ) AS total_contractor_hours,
        SUM(
            COALESCE(fpds.hrs_rn_don_emp, 0) +
            COALESCE(fpds.hrs_rn_admin_emp, 0) +
            COALESCE(fpds.hrs_rn_emp, 0) +
            COALESCE(fpds.hrs_lpn_admin_emp, 0) +
            COALESCE(fpds.hrs_lpn_emp, 0) +
            COALESCE(fpds.hrs_cna_emp, 0) +
            COALESCE(fpds.hrs_nat_rn_emp, 0) +
            COALESCE(fpds.hrs_med_aide_emp, 0)
        ) AS total_employee_hours
    FROM
        HEALTHCARE_DB.SILVER.fact_provider_daily_staffing fpds
    JOIN
        HEALTHCARE_DB.SILVER.dim_provider dp ON fpds.provider_id = dp.provider_id
    GROUP BY
        1,2,3,4
)
SELECT
    provider_id,
    provider_name,
    state,
    month,
    ROUND(((total_contractor_hours / NULLIF((total_employee_hours + total_contractor_hours), 0)) * 100), 2) AS pct_overtime_hours
FROM
    emp_ctr_total_hours
ORDER BY
    1,2,3,4
