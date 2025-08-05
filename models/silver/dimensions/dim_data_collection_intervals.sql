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
         {{ source('bronze', 'nh_data_collection_intervals') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a TRIM() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(MEASURE_CODE) AS measure_code,
        TRIM(MEASURE_DESCRIPTION) AS measure_description,
        TO_DATE(TRIM(DATA_COLLECTION_PERIOD_FROM_DATE), 'MM/DD/YYYY') AS data_collection_period_from_date,
        TO_DATE(TRIM(DATA_COLLECTION_PERIOD_THROUGH_DATE), 'MM/DD/YYYY') AS data_collection_period_through_date,
        TRIM(MEASURE_DATE_RANGE) AS measure_date_range,
        row_number() OVER(PARTITION BY measure_code ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM
        source_data
),
-- Select the fields we want to retain and only retain rows with a rn (row_number() in previous CTE)
-- value of "1" to filter out any duplicated rows.
-- NOTE: Column 'MEASURE_DATE_RANGE' is currently empty.
-- If future values follow a 'MM/DD/YYYY - MM/DD/YYYY' pattern, add parsing logic.
final AS (
    SELECT
        measure_code,
        measure_description,
        data_collection_period_from_date,
        data_collection_period_through_date,
        measure_date_range
    FROM
        deduplicated
    WHERE
        rn = 1
)
-- Select all the retained rows.
SELECT
    measure_code,
    measure_description,
    data_collection_period_from_date,
    data_collection_period_through_date,
    measure_date_range
FROM
    final