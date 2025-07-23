{{ 
    config(
        database='HEALTHCARE_DB',
        schema='SILVER',
        materialized='table',
        tags=['silver', 'healthcare']
    )
}}

-- This model represents swing bed SNF quality data by provider and measure.
-- 
-- Note: Provider attributes such as name, address, region, and contact info
-- are repeated for each row. While this introduces redundancy, we chose to
-- retain these fields in the fact table to reduce modeling complexity and
-- avoid introducing a new dimension table that would only serve this model.
-- 
-- If this dataset were larger or used in high-volume joins, it would be 
-- advisable to extract these fields into a dedicated `dim_swing_bed_provider` 
-- dimension table for normalization and improved storage efficiency.


-- First, grab all the data from our source
WITH source_data AS (
    SELECT
        *
    FROM
         {{ source('bronze', 'swing_bed_snf_data') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a trim() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(CMS_CERTIFICATION_NUMBER) AS provider_id,
        TRIM(PROVIDER_NAME) AS provider_name,
        TRIM(ADDRESS_LINE_1) AS address_line_1,
        TRIM(ADDRESS_LINE_2) AS address_line_2,
        TRIM(CITY_TOWN) AS city_town,
        TRIM(STATE) AS state,
        TRIM(ZIP_CODE) AS zip_code,
        TRIM(COUNTY_PARISH) AS county_parish,
        TRIM(TELEPHONE_NUMBER) AS telephone_number,
        TRIM(CMS_REGION) AS cms_region,
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
        TRIM(MEASUREDATERANGE) AS measure_date_range,
        CASE
            WHEN REGEXP_LIKE(TRIM(MEASUREDATERANGE), '[0-9]{2}/[0-9]{2}/[0-9]{4}-[0-9]{2}/[0-9]{2}/[0-9]{4}') THEN
                TO_DATE(SPLIT_PART(TRIM(MEASUREDATERANGE), '-', 1))
            ELSE NULL
        END AS measure_date_range_start,
        CASE
            WHEN REGEXP_LIKE(TRIM(MEASUREDATERANGE), '[0-9]{2}/[0-9]{2}/[0-9]{4}-[0-9]{2}/[0-9]{2}/[0-9]{4}') THEN
                TO_DATE(SPLIT_PART(TRIM(MEASUREDATERANGE), '-', 2))
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
        provider_name,
        address_line_1,
        address_line_2,
        city_town,
        state,
        zip_code,
        county_parish,
        telephone_number,
        cms_region,
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
     {{ dbt_utils.generate_surrogate_key(['provider_id', 'measure_code']) }} as swing_bed_sk,
    provider_id,
    provider_name,
    address_line_1,
    address_line_2,
    city_town,
    state,
    zip_code,
    county_parish,
    telephone_number,
    cms_region,
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