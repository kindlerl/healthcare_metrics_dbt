{{ 
    config(
        database='HEALTHCARE_DB',
        schema='GOLD',
        materialized='table',
        tags=['gold', 'healthcare']
    )
}}

-- KPI: Patient-to-Nurse Ratio (if available)

-- Due to the lack of individual nurse or patient data, this is calculated as:
-- Number of Substantiated Complaints per 100 Patients (using one-day census 
-- count as a proxy for average patient volume).
-- This provides a reasonable snapshot of complaint rates normalized by facility size.

SELECT
    dp.provider_id,
    dp.provider_name,
    dp.state,
    dpi.number_of_substantiated_complaints,
    fpds.mds_census AS patient_count_on_1970_08_23,
    ROUND(
        dpi.number_of_substantiated_complaints / NULLIF(fpds.mds_census, 0) * 100, 
        2
    ) AS complaints_per_100_patients
FROM
    HEALTHCARE_DB.SILVER.dim_provider_incidents dpi
JOIN
    HEALTHCARE_DB.SILVER.dim_provider dp ON dpi.provider_id = dp.provider_id
JOIN
    HEALTHCARE_DB.SILVER.fact_provider_daily_staffing fpds ON dp.provider_id = fpds.provider_id
WHERE
    dpi.number_of_substantiated_complaints IS NOT NULL
ORDER BY
    complaints_per_100_patients DESC NULLS LAST