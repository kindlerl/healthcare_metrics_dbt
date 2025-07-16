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
        trim(cms_certification_number) AS provider_id,
        trim(provider_name) AS provider_name,
        trim(provider_address) AS provider_address,
        trim(city_town) AS city_town,
        trim(state) AS state,
        trim(zip_code) AS zip_code,
        trim(telephone_number) AS telephone_number,
        trim(county_parish) AS county_name,
        trim(provider_ssa_county_code) AS provider_ssa_county_code,
        trim(ownership_type) AS ownership_type,
        trim(provider_type) AS provider_type,
        trim(legal_business_name) AS legal_business_name,
        trim(provider_resides_in_hospital) AS provider_resides_in_hospital,
        trim(date_first_approved_to_provide_medicare_and_medicaid_services) AS date_first_approved_to_provide_medicare_and_medicaid_services,
        trim(continuing_care_retirement_community) AS continuing_care_retirement_community,
        trim(with_a_resident_and_family_council) AS with_a_resident_and_family_council,
        trim(automatic_sprinkler_systems_in_all_required_areas) AS automatic_sprinkler_systems_in_all_required_areas,
        trim(provider_changed_ownership_in_last_12_months) AS provider_changed_ownership_in_last_12_months,
        trim(location) AS location,
        trim(latitude) AS latitude,
        trim(longitude) AS longitude,
        row_number() OVER(PARTITION BY provider_id ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM
        source_data
),
-- Select the fields we want to retain and only retain rows with a rn (row_number() in previous CTE)
-- value of "1" to filter out any duplicated rows.
final AS (
    SELECT
        provider_id,
        provider_name,
        provider_address,
        city_town,
        state,
        zip_code,
        telephone_number,
        county_name,
        provider_ssa_county_code,
        ownership_type,
        provider_type,
        legal_business_name,
        provider_resides_in_hospital,
        date_first_approved_to_provide_medicare_and_medicaid_services,
        continuing_care_retirement_community,
        with_a_resident_and_family_council,
        automatic_sprinkler_systems_in_all_required_areas,
        provider_changed_ownership_in_last_12_months,
        location,
        latitude,
        longitude
    FROM
        deduplicated
    WHERE
        rn = 1
)
-- Select all the retained rows.
SELECT
    *
FROM
    final;