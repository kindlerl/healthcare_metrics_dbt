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
         {{ source('bronze', 'skilled_nursing_facility_quality_reporting_program_national_data') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a TRIM() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(CMS_CERTIFICATION_NUMBER) AS provider_id,
        TRIM(MEASURE_CODE) AS measure_code,
        CAST(TRIM(SCORE) AS FLOAT) AS score,
        TRIM(FOOTNOTE) AS footnote,
        TO_DATE(TRIM(START_DATE), 'MM/DD/YYYY') AS start_date,
        TO_DATE(TRIM(END_DATE), 'MM/DD/YYYY') AS end_date,
        TRIM(MEASURE_DATE_RANGE) AS measure_date_range,
        row_number() OVER(PARTITION BY provider_id, measure_code ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM
        source_data
),
-- Select the fields we want to retain and only retain rows with a rn (row_number() in previous CTE)
-- value of "1" to filter out any duplicated rows.
-- NOTE: Column 'MEASURE_DATE_RANGE' is currently empty.
-- If future values follow a 'MM/DD/YYYY - MM/DD/YYYY' pattern, add parsing logic.
final AS (
    SELECT
        provider_id,
        measure_code,
        score,
        footnote,
        start_date,
        end_date,
        measure_date_range
    FROM
        deduplicated
    WHERE
        rn = 1
)
-- Select all the retained rows.
SELECT
    {{ dbt_utils.generate_surrogate_key(['provider_id', 'measure_code']) }} as snf_measure_code_sk,
    provider_id,
    measure_code,
    score,
    footnote,
    start_date,
    end_date,
    measure_date_range
FROM
    final
