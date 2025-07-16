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
        trim(CMS_CERTIFICATION_NUMBER) AS provider_id,
        trim(OVERALL_RATING) AS overall_rating,
        trim(OVERALL_RATING_FOOTNOTE) AS overall_rating_footnote,
        trim(HEALTH_INSPECTION_RATING) AS health_inspection_rating,
        trim(HEALTH_INSPECTION_RATING_FOOTNOTE) AS health_inspection_rating_footnote,
        trim(QM_RATING) AS qm_rating,
        trim(QM_RATING_FOOTNOTE) AS qm_rating_footnote,
        trim(LONG_STAY_QM_RATING) AS long_stay_qm_rating,
        trim(LONG_STAY_QM_RATING_FOOTNOTE) AS long_stay_qm_rating_footnote,
        trim(SHORT_STAY_QM_RATING) AS short_stay_qm_rating,
        trim(SHORT_STAY_QM_RATING_FOOTNOTE) AS short_stay_qm_rating_footnote,
        trim(STAFFING_RATING) AS staffing_rating,
        trim(STAFFING_RATING_FOOTNOTE) AS staffing_rating_footnote,
        ROW_NUMBER() OVER(PARTITION BY provider_id ORDER BY LOAD_TIMESTAMP DESC NULLS LAST) AS rn
    FROM
        source_data
),
final AS (
    SELECT
        provider_id,
        overall_rating,
        overall_rating_footnote,
        health_inspection_rating,
        health_inspection_rating_footnote,
        qm_rating,
        qm_rating_footnote,
        long_stay_qm_rating,
        long_stay_qm_rating_footnote,
        short_stay_qm_rating,
        short_stay_qm_rating_footnote,
        staffing_rating,
        staffing_rating_footnote
    FROM
        deduplicated
)
SELECT
    *
FROM
    final;
