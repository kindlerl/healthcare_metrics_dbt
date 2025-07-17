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
        TRIM(NUMBER_OF_FACILITY_REPORTED_INCIDENTS) AS number_of_facility_reported_incidents,
        TRIM(NUMBER_OF_SUBSTANTIATED_COMPLAINTS) AS number_of_substantiated_complaints,
        TRIM(NUMBER_OF_CITATIONS_FROM_INFECTION_CONTROL_INSPECTIONS) AS number_of_citations_from_infection_control_inspections,
        TRIM(NUMBER_OF_FINES) AS number_of_fines,
        TRIM(TOTAL_AMOUNT_OF_FINES_IN_DOLLARS) AS total_amount_of_fines_in_dollars,
        TRIM(NUMBER_OF_PAYMENT_DENIALS) AS number_of_payment_denials,
        TRIM(TOTAL_NUMBER_OF_PENALTIES) AS total_number_of_penalties,
        ROW_NUMBER() OVER(PARTITION BY provider_id ORDER BY LOAD_TIMESTAMP DESC NULLS LAST) AS rn
    FROM
        source_data
),
final AS (
    SELECT
        provider_id,
        CAST(number_of_facility_reported_incidents AS int) AS number_of_facility_reported_incidents,
        CAST(number_of_substantiated_complaints AS int) AS number_of_substantiated_complaints,
        CAST(number_of_citations_from_infection_control_inspections AS int) AS number_of_citations_from_infection_control_inspections,
        CAST(number_of_fines AS int) AS number_of_fines,
        CAST(total_amount_of_fines_in_dollars AS float) AS total_amount_of_fines_in_dollars,
        CAST(number_of_payment_denials AS int) AS number_of_payment_denials,
        CAST(total_number_of_penalties AS int) AS total_number_of_penalties
    FROM
        deduplicated
    WHERE
        rn = 1
)
SELECT
    *
FROM
    final
