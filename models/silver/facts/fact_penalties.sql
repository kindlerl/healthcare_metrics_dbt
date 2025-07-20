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
         {{ source('bronze', 'nh_penalties') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a trim() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(CMS_CERTIFICATION_NUMBER) AS provider_id,
        TO_DATE(TRIM(PENALTY_DATE), 'YYYY-MM-DD') AS penalty_date,
        CAST(TRIM(PENALTY_TYPE) AS VARCHAR) AS penalty_type,
        CAST(TRIM(FINE_AMOUNT) AS FLOAT) AS fine_amount,
        TO_DATE(TRIM(PAYMENT_DENIAL_START_DATE), 'YYYY-MM-DD') AS payment_denial_start_date,
        CAST(TRIM(PAYMENT_DENIAL_LENGTH_IN_DAYS) AS INT) AS payment_denial_length_in_days,
        row_number() OVER(PARTITION BY provider_id, penalty_date ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM
        source_data
),
-- Select the fields we want to retain and only retain rows with a rn (row_number() in previous CTE)
-- value of "1" to filter out any duplicated rows.
final AS (
    SELECT
        provider_id,
        penalty_date,
        penalty_type,
        fine_amount,
        payment_denial_start_date,
        payment_denial_length_in_days
    FROM
        deduplicated
    WHERE
        rn = 1
)
SELECT
    -- Generate a surrogate key from natural key(s)
    {{ dbt_utils.generate_surrogate_key(['provider_id', 'penalty_date']) }} as penalty_sk,
    provider_id,
    penalty_date,
    penalty_type,
    fine_amount,
    payment_denial_start_date,
    payment_denial_length_in_days
FROM
    final