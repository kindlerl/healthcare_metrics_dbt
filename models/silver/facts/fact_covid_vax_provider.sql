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
         {{ source('bronze', 'nh_covid_vax_provider') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a trim() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(CMS_CERTIFICATION_NUMBER) AS provider_id,
        TRIM(STATE) AS state,
        CASE
            WHEN TRIM(PERCENT_OF_RESIDENTS_WHO_ARE_UP_TO_DATE_ON_THEIR_VACCINES) = 'Not Available' THEN NULL
            ELSE CAST(TRIM(PERCENT_OF_RESIDENTS_WHO_ARE_UP_TO_DATE_ON_THEIR_VACCINES) AS FLOAT)
        END AS percent_of_residents_who_are_up_to_date_on_their_vaccines,
        CASE
            WHEN TRIM(PERCENT_OF_STAFF_WHO_ARE_UP_TO_DATE_ON_THEIR_VACCINES) = 'Not Available' THEN NULL
            ELSE CAST(TRIM(PERCENT_OF_STAFF_WHO_ARE_UP_TO_DATE_ON_THEIR_VACCINES) AS FLOAT)
        END AS percent_of_staff_who_are_up_to_date_on_their_vaccines,
        TO_DATE(TRIM(DATE_VACCINATION_DATA_LAST_UPDATED), 'mm/dd/yyyy') AS date_vaccination_data_last_updated,
        row_number() OVER(PARTITION BY provider_id ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM
        source_data
),
-- Select the fields we want to retain and only retain rows with a rn (row_number() in previous CTE)
-- value of "1" to filter out any duplicated rows.
final AS (
    SELECT
        provider_id,
        state,
        percent_of_residents_who_are_up_to_date_on_their_vaccines,
        percent_of_staff_who_are_up_to_date_on_their_vaccines,
        date_vaccination_data_last_updated
    FROM
        deduplicated
    WHERE
        rn = 1
)
SELECT
        provider_id,
        state,
        percent_of_residents_who_are_up_to_date_on_their_vaccines,
        percent_of_staff_who_are_up_to_date_on_their_vaccines,
        date_vaccination_data_last_updated
FROM
    final