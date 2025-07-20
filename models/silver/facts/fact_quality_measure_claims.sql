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
         {{ source('bronze', 'nh_quality_msr_claims') }}
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
        TRIM(MEASURE_DESCRIPTION) AS measure_description,
        TRIM(RESIDENT_TYPE) AS resident_type,
        CAST(TRIM(ADJUSTED_SCORE) AS FLOAT) AS adjusted_score,
        CAST(TRIM(OBSERVED_SCORE) AS FLOAT) AS observed_score,
        CAST(TRIM(EXPECTED_SCORE) AS FLOAT) AS expected_score,
        TRIM(FOOTNOTE_FOR_SCORE) AS footnote_for_score,
        CAST(
            CASE
                WHEN TRIM(USED_IN_QUALITY_MEASURE_FIVE_STAR_RATING) = 'Y' THEN TRUE
                WHEN TRIM(USED_IN_QUALITY_MEASURE_FIVE_STAR_RATING) = 'N' THEN FALSE
                ELSE NULL
            END AS BOOLEAN
        ) AS used_in_quality_measure_five_star_rating,
        TO_DATE(SUBSTRING(TRIM(MEASURE_PERIOD), 1, 8), 'YYYYMMDD') AS measure_period_start,
        TO_DATE(SUBSTRING(TRIM(MEASURE_PERIOD), 10, 8), 'YYYYMMDD') AS measure_period_end,
        -- CASE
        --     WHEN REGEXP_LIKE(TRIM(MEASURE_PERIOD), '^\d{8}-\d{8}$' ) THEN TO_DATE(SUBSTRING(TRIM(MEASURE_PERIOD),1,8), 'YYYYMMDD')
        --     ELSE NULL
        -- END AS measure_period_start,
        -- CASE
        --     WHEN REGEXP_LIKE(TRIM(MEASURE_PERIOD), '^\d{8}-\d{8}$' ) THEN TO_DATE(SUBSTRING(TRIM(MEASURE_PERIOD),10,8), 'YYYYMMDD')
        --     ELSE NULL
        -- END AS measure_period_end,
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
        measure_description,
        resident_type,
        adjusted_score,
        observed_score,
        expected_score,
        footnote_for_score,
        used_in_quality_measure_five_star_rating,
        measure_period_start,
        measure_period_end
    FROM
        deduplicated
    WHERE
        rn = 1
)
SELECT
    provider_id,
    measure_code,
    measure_description,
    resident_type,
    adjusted_score,
    observed_score,
    expected_score,
    footnote_for_score,
    used_in_quality_measure_five_star_rating,
    measure_period_start,
    measure_period_end
FROM
    final