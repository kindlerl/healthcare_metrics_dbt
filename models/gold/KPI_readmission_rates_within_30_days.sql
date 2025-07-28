{{ 
    config(
        database='HEALTHCARE_DB',
        schema='GOLD',
        materialized='table',
        tags=['gold', 'healthcare']
    )
}}

-- KPI: Readmission Rates Within 30 Days by Hospital, State, and Diagnosis Category

-- To be able to accurately calculate this KPI, we would need:

-- Patient-level readmission events ❌
-- Admission and discharge dates ❌
-- Diagnosis category ❌
-- Time-series data (minimum 30 day window) ❌
-- Readmission indicators ❌

-- Since we do not have any of the required metrics, we cannot complete the KPI
-- as requested.

-- HOWEVER, as an alternative (proxy), we can provide information that has already
-- been pre-aggregated and standardized.  There are 2 tables that capture 
-- readmission rates as a general metric:

--      • fact_snf_vbp_facility_performance
--      • dim_snf_vbp_aggregate_performance_metrics

-- Pull the relative columns from these tables 

SELECT
    dp.provider_id,
    dp.provider_name,
    dp.state,
    fp.baseline_period_fy_2019_risk_standardized_readmission_rate AS baseline_readmission_rate,
    fp.performance_period_fy_2022_risk_standardized_readmission_rate AS performance_readmission_rate
FROM
    HEALTHCARE_DB.SILVER.fact_snf_vbp_facility_performance fp
JOIN
    HEALTHCARE_DB.SILVER.dim_provider dp ON fp.provider_id = dp.provider_id
ORDER BY
    performance_readmission_rate ASC NULLS LAST