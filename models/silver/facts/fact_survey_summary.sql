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
         {{ source('bronze', 'nh_survey_summary') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a trim() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(CMS_CERTIFICATION_NUMBER) AS provider_id,
        CAST(TRIM(INSPECTION_CYCLE) AS VARCHAR) AS inspection_cycle,
        TO_DATE(TRIM(HEALTH_SURVEY_DATE), 'YYYY-MM-DD') AS health_survey_date,
        TO_DATE(TRIM(FIRE_SAFETY_SURVEY_DATE), 'YYYY-MM-DD') AS fire_safety_survey_date,
        CAST(TRIM(TOTAL_NUMBER_OF_HEALTH_DEFICIENCIES) AS INT) AS total_number_of_health_deficiencies,
        CAST(TRIM(TOTAL_NUMBER_OF_FIRE_SAFETY_DEFICIENCIES) AS INT) AS total_number_of_fire_safety_deficiencies,
        CAST(TRIM(COUNT_OF_FREEDOM_FROM_ABUSE_AND_NEGLECT_AND_EXPLOITATION_DEFICIENCIES) AS INT) AS count_of_freedom_from_abuse_and_neglect_and_exploitation_deficiencies,
        CAST(TRIM(COUNT_OF_QUALITY_OF_LIFE_AND_CARE_DEFICIENCIES) AS INT) AS count_of_quality_of_life_and_care_deficiencies,
        CAST(TRIM(COUNT_OF_RESIDENT_ASSESSMENT_AND_CARE_PLANNING_DEFICIENCIES) AS INT) AS count_of_resident_assessment_and_care_planning_deficiencies,
        CAST(TRIM(COUNT_OF_NURSING_AND_PHYSICIAN_SERVICES_DEFICIENCIES) AS INT) AS count_of_nursing_and_physician_services_deficiencies,
        CAST(TRIM(COUNT_OF_RESIDENT_RIGHTS_DEFICIENCIES) AS INT) AS count_of_resident_rights_deficiencies,
        CAST(TRIM(COUNT_OF_NUTRITION_AND_DIETARY_DEFICIENCIES) AS INT) AS count_of_nutrition_and_dietary_deficiencies,
        CAST(TRIM(COUNT_OF_PHARMACY_SERVICE_DEFICIENCIES) AS INT) AS count_of_pharmacy_service_deficiencies,
        CAST(TRIM(COUNT_OF_ENVIRONMENTAL_DEFICIENCIES) AS INT) AS count_of_environmental_deficiencies,
        CAST(TRIM(COUNT_OF_ADMINISTRATION_DEFICIENCIES) AS INT) AS count_of_administration_deficiencies,
        CAST(TRIM(COUNT_OF_INFECTION_CONTROL_DEFICIENCIES) AS INT) AS count_of_infection_control_deficiencies,
        CAST(TRIM(COUNT_OF_EMERGENCY_PREPAREDNESS_DEFICIENCIES) AS INT) AS count_of_emergency_preparedness_deficiencies,
        CAST(TRIM(COUNT_OF_AUTOMATIC_SPRINKLER_SYSTEMS_DEFICIENCIES) AS INT) AS count_of_automatic_sprinkler_systems_deficiencies,
        CAST(TRIM(COUNT_OF_CONSTRUCTION_DEFICIENCIES) AS INT) AS count_of_construction_deficiencies,
        CAST(TRIM(COUNT_OF_SERVICES_DEFICIENCIES) AS INT) AS count_of_services_deficiencies,
        CAST(TRIM(COUNT_OF_CORRIDOR_WALLS_AND_DOORS_DEFICIENCIES) AS INT) AS count_of_corridor_walls_and_doors_deficiencies,
        CAST(TRIM(COUNT_OF_EGRESS_DEFICIENCIES) AS INT) AS count_of_egress_deficiencies,
        CAST(TRIM(COUNT_OF_ELECTRICAL_DEFICIENCIES) AS INT) AS count_of_electrical_deficiencies,
        CAST(TRIM(COUNT_OF_EMERGENCY_PLANS_AND_FIRE_DRILLS_DEFICIENCIES) AS INT) AS count_of_emergency_plans_and_fire_drills_deficiencies,
        CAST(TRIM(COUNT_OF_FIRE_ALARM_SYSTEMS_DEFICIENCIES) AS INT) AS count_of_fire_alarm_systems_deficiencies,
        CAST(TRIM(COUNT_OF_SMOKE_DEFICIENCIES) AS INT) AS count_of_smoke_deficiencies,
        CAST(TRIM(COUNT_OF_INTERIOR_DEFICIENCIES) AS INT) AS count_of_interior_deficiencies,
        CAST(TRIM(COUNT_OF_GAS_AND_VACUUM_AND_ELECTRICAL_SYSTEMS_DEFICIENCIES) AS INT) AS count_of_gas_and_vacuum_and_electrical_systems_deficiencies,
        CAST(TRIM(COUNT_OF_HAZARDOUS_AREA_DEFICIENCIES) AS INT) AS count_of_hazardous_area_deficiencies,
        CAST(TRIM(COUNT_OF_ILLUMINATION_AND_EMERGENCY_POWER_DEFICIENCIES) AS INT) AS count_of_illumination_and_emergency_power_deficiencies,
        CAST(TRIM(COUNT_OF_LABORATORIES_DEFICIENCIES) AS INT) AS count_of_laboratories_deficiencies,
        CAST(TRIM(COUNT_OF_MEDICAL_GASES_AND_ANAESTHETIZING_AREAS_DEFICIENCIES) AS INT) AS count_of_medical_gases_and_anaesthetizing_areas_deficiencies,
        CAST(TRIM(COUNT_OF_SMOKING_REGULATIONS_DEFICIENCIES) AS INT) AS count_of_smoking_regulations_deficiencies,
        CAST(TRIM(COUNT_OF_MISCELLANEOUS_DEFICIENCIES) AS INT) AS count_of_miscellaneous_deficiencies,
        row_number() OVER(PARTITION BY provider_id, inspection_cycle ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM
        source_data
),
-- Select the fields we want to retain and only retain rows with a rn (row_number() in previous CTE)
-- value of "1" to filter out any duplicated rows.
final AS (
    SELECT
        provider_id,
        inspection_cycle,
        health_survey_date,
        fire_safety_survey_date,
        total_number_of_health_deficiencies,
        total_number_of_fire_safety_deficiencies,
        count_of_freedom_from_abuse_and_neglect_and_exploitation_deficiencies,
        count_of_quality_of_life_and_care_deficiencies,
        count_of_resident_assessment_and_care_planning_deficiencies,
        count_of_nursing_and_physician_services_deficiencies,
        count_of_resident_rights_deficiencies,
        count_of_nutrition_and_dietary_deficiencies,
        count_of_pharmacy_service_deficiencies,
        count_of_environmental_deficiencies,
        count_of_administration_deficiencies,
        count_of_infection_control_deficiencies,
        count_of_emergency_preparedness_deficiencies,
        count_of_automatic_sprinkler_systems_deficiencies,
        count_of_construction_deficiencies,
        count_of_services_deficiencies,
        count_of_corridor_walls_and_doors_deficiencies,
        count_of_egress_deficiencies,
        count_of_electrical_deficiencies,
        count_of_emergency_plans_and_fire_drills_deficiencies,
        count_of_fire_alarm_systems_deficiencies,
        count_of_smoke_deficiencies,
        count_of_interior_deficiencies,
        count_of_gas_and_vacuum_and_electrical_systems_deficiencies,
        count_of_hazardous_area_deficiencies,
        count_of_illumination_and_emergency_power_deficiencies,
        count_of_laboratories_deficiencies,
        count_of_medical_gases_and_anaesthetizing_areas_deficiencies,
        count_of_smoking_regulations_deficiencies,
        count_of_miscellaneous_deficiencies
    FROM
        deduplicated
    WHERE
        rn = 1
)
SELECT
    -- Generate a surrogate key from natural key(s)
    {{ dbt_utils.generate_surrogate_key(['provider_id', 'inspection_cycle']) }} as survey_summary__sk,
    provider_id,
    inspection_cycle,
    health_survey_date,
    fire_safety_survey_date,
    total_number_of_health_deficiencies,
    total_number_of_fire_safety_deficiencies,
    count_of_freedom_from_abuse_and_neglect_and_exploitation_deficiencies,
    count_of_quality_of_life_and_care_deficiencies,
    count_of_resident_assessment_and_care_planning_deficiencies,
    count_of_nursing_and_physician_services_deficiencies,
    count_of_resident_rights_deficiencies,
    count_of_nutrition_and_dietary_deficiencies,
    count_of_pharmacy_service_deficiencies,
    count_of_environmental_deficiencies,
    count_of_administration_deficiencies,
    count_of_infection_control_deficiencies,
    count_of_emergency_preparedness_deficiencies,
    count_of_automatic_sprinkler_systems_deficiencies,
    count_of_construction_deficiencies,
    count_of_services_deficiencies,
    count_of_corridor_walls_and_doors_deficiencies,
    count_of_egress_deficiencies,
    count_of_electrical_deficiencies,
    count_of_emergency_plans_and_fire_drills_deficiencies,
    count_of_fire_alarm_systems_deficiencies,
    count_of_smoke_deficiencies,
    count_of_interior_deficiencies,
    count_of_gas_and_vacuum_and_electrical_systems_deficiencies,
    count_of_hazardous_area_deficiencies,
    count_of_illumination_and_emergency_power_deficiencies,
    count_of_laboratories_deficiencies,
    count_of_medical_gases_and_anaesthetizing_areas_deficiencies,
    count_of_smoking_regulations_deficiencies,
    count_of_miscellaneous_deficiencies
FROM
    final