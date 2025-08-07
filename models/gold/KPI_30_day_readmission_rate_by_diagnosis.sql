{{ 
    config(
        database='HEALTHCARE_DB',
        schema='GOLD',
        materialized='table',
        tags=['gold', 'healthcare']
    )
}}

-- KPI: 30-Day Readmission Rates by Diagnosis
--
-- This KPI pulls 4 specific aggregated metrics from our data set
-- to satisfy the request.  These metrics represent the most recent 
-- available value as reported by CMS. Time periods may vary by 
-- measure source.

SELECT
    MEASURE_DESCRIPTION AS  METRIC_NAME,
    'CLAIMS' AS METRIC_SOURCE,
    AVG(adjusted_score) AS AVG_SCORE,
    CURRENT_TIMESTAMP() AS LAST_UPDATED
FROM
    {{ ref('fact_quality_measure_claims') }}
WHERE
    MEASURE_DESCRIPTION IN (
        'Percentage of short-stay residents who were rehospitalized after a nursing home admission',
        'Percentage of short-stay residents who had an outpatient emergency department visit',
        'Number of outpatient emergency department visits per 1000 long-stay resident days'
    )
GROUP BY MEASURE_DESCRIPTION

UNION ALL

SELECT  
    'Number of hospitalizations per 1000 long-stay resident days' AS METRIC_NAME,
    'STATE_US_AVERAGES' AS METRIC_SOURCE,
    NUMBER_OF_HOSPITALIZATIONS_PER_1000_LONG_STAY_RESIDENT_DAYS AS AVG_SCORE,
    CURRENT_TIMESTAMP() AS LAST_UPDATED
FROM 
    {{ ref('fact_state_us_averages') }}
WHERE 
    STATE_OR_NATION = 'NATION'  -- Use the National metric which is already aggregated  

