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
         {{ source('bronze', 'nh_provider_info') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a trim() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(cms_certification_number) AS provider_id,
        TRIM(RATING_CYCLE_1_STANDARD_SURVEY_HEALTH_DATE) AS rating_cycle_1_standard_survey_health_date,
        TRIM(RATING_CYCLE_1_TOTAL_NUMBER_OF_HEALTH_DEFICIENCIES) AS rating_cycle_1_total_number_of_health_deficiencies,
        TRIM(RATING_CYCLE_1_NUMBER_OF_STANDARD_HEALTH_DEFICIENCIES) AS rating_cycle_1_number_of_standard_health_deficiencies,
        TRIM(RATING_CYCLE_1_NUMBER_OF_COMPLAINT_HEALTH_DEFICIENCIES) AS rating_cycle_1_number_of_complaint_health_deficiencies,
        TRIM(RATING_CYCLE_1_HEALTH_DEFICIENCY_SCORE) AS rating_cycle_1_health_deficiency_score,
        TRIM(RATING_CYCLE_1_NUMBER_OF_HEALTH_REVISITS) AS rating_cycle_1_number_of_health_revisits,
        TRIM(RATING_CYCLE_1_HEALTH_REVISIT_SCORE) AS rating_cycle_1_health_revisit_score,
        TRIM(RATING_CYCLE_1_TOTAL_HEALTH_SCORE) AS rating_cycle_1_total_health_score,
        TRIM(RATING_CYCLE_2_STANDARD_HEALTH_SURVEY_DATE) AS rating_cycle_2_standard_health_survey_date,
        TRIM(RATING_CYCLE_2_TOTAL_NUMBER_OF_HEALTH_DEFICIENCIES) AS rating_cycle_2_total_number_of_health_deficiencies,
        TRIM(RATING_CYCLE_2_NUMBER_OF_STANDARD_HEALTH_DEFICIENCIES) AS rating_cycle_2_number_of_standard_health_deficiencies,
        TRIM(RATING_CYCLE_2_NUMBER_OF_COMPLAINT_HEALTH_DEFICIENCIES) AS rating_cycle_2_number_of_complaint_health_deficiencies,
        TRIM(RATING_CYCLE_2_HEALTH_DEFICIENCY_SCORE) AS rating_cycle_2_health_deficiency_score,
        TRIM(RATING_CYCLE_2_NUMBER_OF_HEALTH_REVISITS) AS rating_cycle_2_number_of_health_revisits,
        TRIM(RATING_CYCLE_2_HEALTH_REVISIT_SCORE) AS rating_cycle_2_health_revisit_score,
        TRIM(RATING_CYCLE_2_TOTAL_HEALTH_SCORE) AS rating_cycle_2_total_health_score,
        TRIM(RATING_CYCLE_3_STANDARD_HEALTH_SURVEY_DATE) AS rating_cycle_3_standard_health_survey_date,
        TRIM(RATING_CYCLE_3_TOTAL_NUMBER_OF_HEALTH_DEFICIENCIES) AS rating_cycle_3_total_number_of_health_deficiencies,
        TRIM(RATING_CYCLE_3_NUMBER_OF_STANDARD_HEALTH_DEFICIENCIES) AS rating_cycle_3_number_of_standard_health_deficiencies,
        TRIM(RATING_CYCLE_3_NUMBER_OF_COMPLAINT_HEALTH_DEFICIENCIES) AS rating_cycle_3_number_of_complaint_health_deficiencies,
        TRIM(RATING_CYCLE_3_HEALTH_DEFICIENCY_SCORE) AS rating_cycle_3_health_deficiency_score,
        TRIM(RATING_CYCLE_3_NUMBER_OF_HEALTH_REVISITS) AS rating_cycle_3_number_of_health_revisits,
        TRIM(RATING_CYCLE_3_HEALTH_REVISIT_SCORE) AS rating_cycle_3_health_revisit_score,
        TRIM(RATING_CYCLE_3_TOTAL_HEALTH_SCORE) AS rating_cycle_3_total_health_score,
        TRIM(TOTAL_WEIGHTED_HEALTH_SURVEY_SCORE) AS total_weighted_health_survey_score,        
        row_number() OVER(PARTITION BY provider_id ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM
        source_data
),
-- Select the fields we want to retain and only retain rows with a rn (row_number() in previous CTE)
-- value of "1" to filter out any duplicated rows.
final AS (
    SELECT
        provider_id,
        CAST(rating_cycle_1_standard_survey_health_date AS date) AS rating_cycle_1_standard_survey_health_date,
        CAST(rating_cycle_1_total_number_of_health_deficiencies AS int) AS rating_cycle_1_total_number_of_health_deficiencies,
        CAST(rating_cycle_1_number_of_standard_health_deficiencies AS int) AS rating_cycle_1_number_of_standard_health_deficiencies,
        CAST(rating_cycle_1_number_of_complaint_health_deficiencies AS int) AS rating_cycle_1_number_of_complaint_health_deficiencies,
        CAST(rating_cycle_1_health_deficiency_score AS int) AS rating_cycle_1_health_deficiency_score,
        CAST(rating_cycle_1_number_of_health_revisits AS int) AS rating_cycle_1_number_of_health_revisits,
        CAST(rating_cycle_1_health_revisit_score AS int) AS rating_cycle_1_health_revisit_score,
        CAST(rating_cycle_1_total_health_score AS int) AS rating_cycle_1_total_health_score,
        CAST(rating_cycle_2_standard_health_survey_date AS date) AS rating_cycle_2_standard_health_survey_date,
        CAST(rating_cycle_2_total_number_of_health_deficiencies AS int) AS rating_cycle_2_total_number_of_health_deficiencies,
        CAST(rating_cycle_2_number_of_standard_health_deficiencies AS int) AS rating_cycle_2_number_of_standard_health_deficiencies,
        CAST(rating_cycle_2_number_of_complaint_health_deficiencies AS int) AS rating_cycle_2_number_of_complaint_health_deficiencies,
        CAST(rating_cycle_2_health_deficiency_score AS int) AS rating_cycle_2_health_deficiency_score,
        CAST(rating_cycle_2_number_of_health_revisits AS int) AS rating_cycle_2_number_of_health_revisits,
        CAST(rating_cycle_2_health_revisit_score AS int) AS rating_cycle_2_health_revisit_score,
        CAST(rating_cycle_2_total_health_score AS int) AS rating_cycle_2_total_health_score,
        CAST(rating_cycle_3_standard_health_survey_date AS date) AS rating_cycle_3_standard_health_survey_date,
        CAST(rating_cycle_3_total_number_of_health_deficiencies AS int) AS rating_cycle_3_total_number_of_health_deficiencies,
        CAST(rating_cycle_3_number_of_standard_health_deficiencies AS int) AS rating_cycle_3_number_of_standard_health_deficiencies,
        CAST(rating_cycle_3_number_of_complaint_health_deficiencies AS int) AS rating_cycle_3_number_of_complaint_health_deficiencies,
        CAST(rating_cycle_3_health_deficiency_score AS int) AS rating_cycle_3_health_deficiency_score,
        CAST(rating_cycle_3_number_of_health_revisits AS int) AS rating_cycle_3_number_of_health_revisits,
        CAST(rating_cycle_3_health_revisit_score AS int) AS rating_cycle_3_health_revisit_score,
        CAST(rating_cycle_3_total_health_score AS int) AS rating_cycle_3_total_health_score,
        CAST(total_weighted_health_survey_score AS float) AS total_weighted_health_survey_score
    FROM
        deduplicated
    WHERE
        rn = 1
)
-- Select all the retained rows.
SELECT
    {{ dbt_utils.generate_surrogate_key(['provider_id']) }} as provider_sk,
    *
FROM
    final