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
        TRIM(CMS_CERTIFICATION_NUMBER) AS provider_id,
         CAST(TRIM(NUMBER_OF_FACILITY_REPORTED_INCIDENTS) AS int) AS number_of_facility_reported_incidents,
         CAST(TRIM(NUMBER_OF_SUBSTANTIATED_COMPLAINTS) AS int) AS number_of_substantiated_complaints,
         CAST(TRIM(NUMBER_OF_CITATIONS_FROM_INFECTION_CONTROL_INSPECTIONS) AS int) AS number_of_citations_from_infection_control_inspections,
         CAST(TRIM(NUMBER_OF_FINES) AS int) AS number_of_fines,
         CAST(TRIM(TOTAL_AMOUNT_OF_FINES_IN_DOLLARS) AS int) AS total_amount_of_fines_in_dollars,
         CAST(TRIM(NUMBER_OF_PAYMENT_DENIALS) AS int) AS number_of_payment_denials,
         CAST(TRIM(TOTAL_NUMBER_OF_PENALTIES) AS int) AS total_number_of_penalties,
        ROW_NUMBER() OVER(PARTITION BY provider_id ORDER BY LOAD_TIMESTAMP DESC NULLS LAST) AS rn
    FROM
        source_data
),
final AS (
    SELECT
        provider_id,
        number_of_facility_reported_incidents,
        number_of_substantiated_complaints,
        number_of_citations_from_infection_control_inspections,
        number_of_fines,
        total_amount_of_fines_in_dollars,
        number_of_payment_denials,
        total_number_of_penalties
    FROM
        deduplicated
    WHERE
        rn = 1
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['provider_id']) }} as provider_sk,
    provider_id,
    number_of_facility_reported_incidents,
    number_of_substantiated_complaints,
    number_of_citations_from_infection_control_inspections,
    number_of_fines,
    total_amount_of_fines_in_dollars,
    number_of_payment_denials,
    total_number_of_penalties
FROM
    final
