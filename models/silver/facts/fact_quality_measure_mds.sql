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
         {{ source('bronze', 'nh_quality_msr_mds') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a trim() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(CMS_CERTIFICATION_NUMBER) AS provider_id,
        CAST(TRIM(MEASURE_CODE) AS VARCHAR) AS measure_code,
        CAST(TRIM(MEASURE_DESCRIPTION) AS VARCHAR) AS measure_description,
        CAST(TRIM(RESIDENT_TYPE) AS VARCHAR) AS resident_type,
        CAST(TRIM(Q1_MEASURE_SCORE) AS FLOAT) AS q1_measure_score,
        CAST(TRIM(FOOTNOTE_FOR_Q1_MEASURE_SCORE) AS VARCHAR) AS footnote_for_q1_measure_score,
        CAST(TRIM(Q2_MEASURE_SCORE) AS FLOAT) AS q2_measure_score,
        CAST(TRIM(FOOTNOTE_FOR_Q2_MEASURE_SCORE) AS VARCHAR) AS footnote_for_q2_measure_score,
        CAST(TRIM(Q3_MEASURE_SCORE) AS FLOAT) AS q3_measure_score,
        CAST(TRIM(FOOTNOTE_FOR_Q3_MEASURE_SCORE) AS VARCHAR) AS footnote_for_q3_measure_score,
        CAST(TRIM(Q4_MEASURE_SCORE) AS FLOAT) AS q4_measure_score,
        CAST(TRIM(FOOTNOTE_FOR_Q4_MEASURE_SCORE) AS VARCHAR) AS footnote_for_q4_measure_score,
        CAST(TRIM(FOUR_QUARTER_AVERAGE_SCORE) AS FLOAT) AS four_quarter_average_score,
        CAST(TRIM(FOOTNOTE_FOR_FOUR_QUARTER_AVERAGE_SCORE) AS VARCHAR) AS footnote_for_four_quarter_average_score,
        CAST(
            CASE
                WHEN USED_IN_QUALITY_MEASURE_FIVE_STAR_RATING = 'Y' THEN TRUE
                WHEN USED_IN_QUALITY_MEASURE_FIVE_STAR_RATING = 'N' THEN FALSE
                ELSE NULL
            END AS BOOLEAN
        ) AS used_in_quality_measure_five_star_rating,
        SPLIT_PART(MEASURE_PERIOD, '-', 1) AS measure_period_start_qtr,
        SPLIT_PART(MEASURE_PERIOD, '-', 2) AS measure_period_end_qtr,
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
        q1_measure_score,
        footnote_for_q1_measure_score,
        q2_measure_score,
        footnote_for_q2_measure_score,
        q3_measure_score,
        footnote_for_q3_measure_score,
        q4_measure_score,
        footnote_for_q4_measure_score,
        four_quarter_average_score,
        footnote_for_four_quarter_average_score,
        used_in_quality_measure_five_star_rating,
        measure_period_start_qtr,
        measure_period_end_qtr
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
    q1_measure_score,
    footnote_for_q1_measure_score,
    q2_measure_score,
    footnote_for_q2_measure_score,
    q3_measure_score,
    footnote_for_q3_measure_score,
    q4_measure_score,
    footnote_for_q4_measure_score,
    four_quarter_average_score,
    footnote_for_four_quarter_average_score,
    used_in_quality_measure_five_star_rating,
    measure_period_start_qtr,
    measure_period_end_qtr
FROM
    final