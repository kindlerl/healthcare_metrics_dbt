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
        TRIM(reported_nurse_aide_staffing_hours_per_resident_per_day) AS reported_nurse_aide_staffing_hours_per_resident_per_day,
        TRIM(reported_lpn_staffing_hours_per_resident_per_day) AS reported_lpn_staffing_hours_per_resident_per_day,
        TRIM(reported_rn_staffing_hours_per_resident_per_day) AS reported_rn_staffing_hours_per_resident_per_day,
        TRIM(reported_licensed_staffing_hours_per_resident_per_day) AS reported_licensed_staffing_hours_per_resident_per_day,
        TRIM(reported_total_nurse_staffing_hours_per_resident_per_day) AS reported_total_nurse_staffing_hours_per_resident_per_day,
        row_number() OVER(PARTITION BY provider_id ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM
        source_data
),
-- Select the fields we want to retain and only retain rows with a rn (row_number() in previous CTE)
-- value of "1" to filter out any duplicated rows.
final AS (
    SELECT
        provider_id,
        CAST(reported_nurse_aide_staffing_hours_per_resident_per_day AS float) AS reported_nurse_aide_staffing_hours_per_resident_per_day,
        CAST(reported_lpn_staffing_hours_per_resident_per_day AS float) AS reported_lpn_staffing_hours_per_resident_per_day,
        CAST(reported_rn_staffing_hours_per_resident_per_day AS float) AS reported_rn_staffing_hours_per_resident_per_day,
        CAST(reported_licensed_staffing_hours_per_resident_per_day AS float) AS reported_licensed_staffing_hours_per_resident_per_day,
        CAST(reported_total_nurse_staffing_hours_per_resident_per_day AS float) AS reported_total_nurse_staffing_hours_per_resident_per_day
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