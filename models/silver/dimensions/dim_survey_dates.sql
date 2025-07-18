{{ 
    config(
        database='HEALTHCARE_DB',
        schema='SILVER',
        materialized='table',
        tags=['silver', 'healthcare']
    )
}}

-- First, grab all the data from our source
WITH source_data AS (
    SELECT
        *
    FROM
         {{ source('bronze', 'nh_survey_dates') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a trim() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.

deduplicated AS (
    SELECT
        TRIM(CMS_CERTIFICATION_NUMBER) AS provider_id,
        CAST(TRIM(SURVEY_DATE) AS DATE) AS survey_date,
        TRIM(TYPE_OF_SURVEY) AS survey_type,
        CAST(TRIM(SURVEY_CYCLE) AS INT) AS survey_cycle,
        ROW_NUMBER() OVER(PARTITION BY provider_id ORDER BY LOAD_TIMESTAMP DESC NULLS LAST) AS rn
    FROM
        source_data
),
final AS (
    SELECT
        provider_id,
        survey_date,
        survey_type,
        survey_cycle
    FROM
        deduplicated
    WHERE
        rn = 1
    AND
        provider_id IS NOT NULL
)
-- After looking at the data, I determined that a single provider can have multiple entries
-- for the same survey_date.  These multiple entries are differentiated by BOTH the type
-- of survey AND the survey cycle.  Include all 4 fields in the surrogate key.
SELECT
    {{ dbt_utils.generate_surrogate_key(['provider_id', 'survey_date', 'survey_cycle', 'survey_type']) }} as survey_date_sk,
    provider_id,
    survey_date,
    survey_type,
    survey_cycle
FROM
    final
