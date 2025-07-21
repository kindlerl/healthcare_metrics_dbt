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
         {{ source('bronze', 'skilled_nursing_facility_quality_reporting_program_provider_data') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a trim() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(CMS_CERTIFICATION_NUMBER) AS provider_id,
        TRIM(MEASURE_CODE) AS measure_code,
        CASE
            WHEN REGEXP_LIKE(TRIM(SCORE), '^[0-9].*?$') THEN CAST(TRIM(SCORE) AS FLOAT)
            ELSE NULL
        END AS score,
        CASE
            WHEN REGEXP_LIKE(TRIM(SCORE), '^[0-9].*?$') THEN NULL
            ELSE TRIM(SCORE)
        END AS score_interpretation,
        TRIM(FOOTNOTE) AS footnote,
        TO_DATE(TRIM(START_DATE), 'MM/DD/YYYY') AS start_date,
        TO_DATE(TRIM(END_DATE), 'MM/DD/YYYY') AS end_date,
        TRIM(MEASURE_DATE_RANGE) AS measure_date_range,
        CASE
            WHEN REGEXP_LIKE(TRIM(MEASURE_DATE_RANGE), '[0-9]{2}/[0-9]{2}/[0-9]{4}-[0-9]{2}/[0-9]{2}/[0-9]{4}') THEN
                TO_DATE(SPLIT_PART(TRIM(MEASURE_DATE_RANGE), '-', 1))
            ELSE NULL
        END AS measure_date_range_start,
        CASE
            WHEN REGEXP_LIKE(TRIM(MEASURE_DATE_RANGE), '[0-9]{2}/[0-9]{2}/[0-9]{4}-[0-9]{2}/[0-9]{2}/[0-9]{4}') THEN
                TO_DATE(SPLIT_PART(TRIM(MEASURE_DATE_RANGE), '-', 2))
            ELSE NULL
        END AS measure_date_range_end,
        row_number() OVER(PARTITION BY provider_id, measure_code ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM
        source_data
),
-- Select the fields we want to retain and only retain rows with a rn (row_number() in previous CTE)
-- value of "1" to filter out any duplicated rows.
final AS (
    SELECT
        provider_id,
        measure_code,
        score,
        score_interpretation,
        footnote,
        start_date,
        end_date,
        measure_date_range,
        measure_date_range_start,
        measure_date_range_end
    FROM
        deduplicated
    WHERE
        rn = 1
)
SELECT
     {{ dbt_utils.generate_surrogate_key(['provider_id', 'measure_code']) }} as snf_provider_sk,
    provider_id,
    measure_code,
    score,
    score_interpretation,
    footnote,
    start_date,
    end_date,
    measure_date_range,
    measure_date_range_start,
    measure_date_range_end
FROM
    final