{{ 
    config(
        database='HEALTHCARE_DB',
        schema='GOLD',
        materialized='table',
        tags=['gold', 'healthcare']
    )
}}

-- KPI: Ratio of Permanent Staff to Contract Staff

-- While we do not have the adequate metrics to calculate employment start/stop
-- dates for both employees and contractors, we do have an hours breakdown of
-- each for a single day (1970-08-23). We can use the hours breakdown of each
-- nurse type (employee, contractor) to proxy a relationship.

WITH staffing_breakdown AS (
    SELECT
        fpds.provider_id,
        dp.provider_name,
        dp.state,
        -- Sum of all employee hours
        COALESCE(HRS_RN_DON_EMP, 0) + COALESCE(HRS_RN_ADMIN_EMP, 0) + COALESCE(HRS_RN_EMP, 0) +
        COALESCE(HRS_LPN_ADMIN_EMP, 0) + COALESCE(HRS_LPN_EMP, 0) + COALESCE(HRS_CNA_EMP, 0) + 
        COALESCE(HRS_NAT_RN_EMP, 0) + COALESCE(HRS_MED_AIDE_EMP, 0)
        AS total_employee_hours,
        
        -- Sum of all contractor hours
        COALESCE(HRS_RN_DON_CTR, 0) + COALESCE(HRS_RN_ADMIN_CTR, 0) + COALESCE(HRS_RN_CTR, 0) +
        COALESCE(HRS_LPN_ADMIN_CTR, 0) + COALESCE(HRS_LPN_CTR, 0) + COALESCE(HRS_CNA_CTR, 0) + 
        COALESCE(HRS_NAT_RN_CTR, 0) + COALESCE(HRS_MED_AIDE_CTR, 0)
        AS total_contractor_hours
    FROM
        HEALTHCARE_DB.SILVER.fact_provider_daily_staffing fpds
    JOIN
        HEALTHCARE_DB.SILVER.dim_provider dp ON fpds.provider_id = dp.provider_id
    WHERE
        fpds.staffing_date = '1970-08-23'
)
SELECT
    provider_id,
    provider_name,
    state,
    total_employee_hours,
    total_contractor_hours,
    ROUND(
        total_employee_hours / NULLIF(total_contractor_hours, 0), 
        2
    ) AS perm_to_contract_ratio
FROM
    staffing_breakdown
ORDER BY
    perm_to_contract_ratio DESC NULLS LAST