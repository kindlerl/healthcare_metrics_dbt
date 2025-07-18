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
-- we want to retain, applying a TRIM() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(CMS_CERTIFICATION_NUMBER) AS provider_id,
        CAST(TRIM(OVERALL_RATING) AS int) AS overall_rating,
        TRIM(OVERALL_RATING_FOOTNOTE) AS overall_rating_footnote,
        CAST(TRIM(HEALTH_INSPECTION_RATING) AS int) AS health_inspection_rating,
        TRIM(HEALTH_INSPECTION_RATING_FOOTNOTE) AS health_inspection_rating_footnote,
        CAST(TRIM(QM_RATING) AS int) AS qm_rating,
        TRIM(QM_RATING_FOOTNOTE) AS qm_rating_footnote,
        CAST(TRIM(LONG_STAY_QM_RATING) AS int) AS long_stay_qm_rating,
        TRIM(LONG_STAY_QM_RATING_FOOTNOTE) AS long_stay_qm_rating_footnote,
        CAST(TRIM(SHORT_STAY_QM_RATING) AS int) AS short_stay_qm_rating,
        TRIM(SHORT_STAY_QM_RATING_FOOTNOTE) AS short_stay_qm_rating_footnote,
        CAST(TRIM(STAFFING_RATING) AS int) AS staffing_rating,
        TRIM(STAFFING_RATING_FOOTNOTE) AS staffing_rating_footnote,
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
    WHERE
        rn = 1
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['provider_id']) }} as provider_sk,
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
    final
