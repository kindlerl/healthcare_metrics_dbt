{{ 
    config(
        database='HEALTHCARE_DB',
        schema='SILVER',
        materialized='table',
        tags=['silver', 'healthcare']
    )
}}

-- First, grab all the data from our source
WITH provider_info_source_data AS (
    SELECT
        *
    FROM
         {{ source('bronze', 'nh_provider_info') }}
),
snf_provider_data AS (
    SELECT
        *
    FROM
        {{ source('bronze', 'skilled_nursing_facility_quality_reporting_program_provider_data') }}
),
source_data AS (
    SELECT
        pisd.*,
        spd.cms_region
    FROM
        provider_info_source_data pisd
    LEFT JOIN
        snf_provider_data spd
    ON
        pisd.CMS_CERTIFICATION_NUMBER = spd.CMS_CERTIFICATION_NUMBER
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a TRIM() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(cms_certification_number) AS provider_id,
        TRIM(provider_name) AS provider_name,
        TRIM(provider_address) AS provider_address,
        TRIM(city_town) AS city_town,
        TRIM(state) AS state,
        TRIM(zip_code) AS zip_code,
        TRIM(telephone_number) AS telephone_number,
        TRIM(county_parish) AS county_name,
        TRIM(cms_region) AS cms_region,
        TRIM(provider_ssa_county_code) AS provider_ssa_county_code,
        TRIM(ownership_type) AS ownership_type,
        TRIM(provider_type) AS provider_type,
        TRIM(legal_business_name) AS legal_business_name,
        TRIM(provider_resides_in_hospital) AS provider_resides_in_hospital,
        TO_DATE(TRIM(date_first_approved_to_provide_medicare_and_medicaid_services), 'YYYY-MM-DD') AS date_first_approved_to_provide_medicare_and_medicaid_services,
        TRIM(continuing_care_retirement_community) AS continuing_care_retirement_community,
        TRIM(with_a_resident_and_family_council) AS with_a_resident_and_family_council,
        TRIM(automatic_sprinkler_systems_in_all_required_areas) AS automatic_sprinkler_systems_in_all_required_areas,
        TRIM(provider_changed_ownership_in_last_12_months) AS provider_changed_ownership_in_last_12_months,
        CAST(TRIM(latitude) AS FLOAT) AS latitude,
        CAST(TRIM(longitude) AS FLOAT) AS longitude,
        row_number() OVER(PARTITION BY provider_id, cms_region ORDER BY load_timestamp DESC NULLS LAST) AS rn
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
        cms_region,
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
        latitude,
        longitude
    FROM
        deduplicated
    WHERE
        rn = 1
)
-- Select all the retained rows.
-- 12:58:29 Encountered an error:
-- Compilation Error in model dim_provider (models/silver/dimensions/dim_provider.sql)
  
-- Warning: `dbt_utils.surrogate_key` has been replaced by `dbt_utils.generate_surrogate_key`. 
-- The new macro treats null values differently to empty strings. To restore the behaviour of 
-- the original macro, add a global variable in dbt_project.yml called 
-- `surrogate_key_treat_nulls_as_empty_strings` to your dbt_project.yml file with a value of 
-- True. The dbt_healthcare.dim_provider model triggered this warning. 
  
--   > in macro default__surrogate_key (macros/sql/surrogate_key.sql)
--   > called by macro surrogate_key (macros/sql/surrogate_key.sql)
--   > called by model dim_provider (models/silver/dimensions/dim_provider.sql)
SELECT
    {{ dbt_utils.generate_surrogate_key(['provider_id']) }} as provider_sk,
    provider_id,
    provider_name,
    provider_address,
    city_town,
    state,
    zip_code,
    telephone_number,
    county_name,
    cms_region,
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
    latitude,
    longitude
FROM
    final