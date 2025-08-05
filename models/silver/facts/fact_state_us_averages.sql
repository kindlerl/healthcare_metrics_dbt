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
         {{ source('bronze', 'nh_state_us_averages') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a trim() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(STATE_OR_NATION) AS state_or_nation,
        CAST(TRIM(CYCLE_1_TOTAL_NUMBER_OF_HEALTH_DEFICIENCIES) AS FLOAT) AS cycle_1_total_number_of_health_deficiencies,
        CAST(TRIM(CYCLE_1_TOTAL_NUMBER_OF_FIRE_SAFETY_DEFICIENCIES) AS FLOAT) AS cycle_1_total_number_of_fire_safety_deficiencies,
        CAST(TRIM(CYCLE_2_TOTAL_NUMBER_OF_HEALTH_DEFICIENCIES) AS FLOAT) AS cycle_2_total_number_of_health_deficiencies,
        CAST(TRIM(CYCLE_2_TOTAL_NUMBER_OF_FIRE_SAFETY_DEFICIENCIES) AS FLOAT) AS cycle_2_total_number_of_fire_safety_deficiencies,
        CAST(TRIM(CYCLE_3_TOTAL_NUMBER_OF_HEALTH_DEFICIENCIES) AS FLOAT) AS cycle_3_total_number_of_health_deficiencies,
        CAST(TRIM(CYCLE_3_TOTAL_NUMBER_OF_FIRE_SAFETY_DEFICIENCIES) AS FLOAT) AS cycle_3_total_number_of_fire_safety_deficiencies,
        CAST(TRIM(AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY) AS FLOAT) AS average_number_of_residents_per_day,
        CAST(TRIM(REPORTED_NURSE_AIDE_STAFFING_HOURS_PER_RESIDENT_PER_DAY) AS FLOAT) AS reported_nurse_aide_staffing_hours_per_resident_per_day,
        CAST(TRIM(REPORTED_LPN_STAFFING_HOURS_PER_RESIDENT_PER_DAY) AS FLOAT) AS reported_lpn_staffing_hours_per_resident_per_day,
        CAST(TRIM(REPORTED_RN_STAFFING_HOURS_PER_RESIDENT_PER_DAY) AS FLOAT) AS reported_rn_staffing_hours_per_resident_per_day,
        CAST(TRIM(REPORTED_LICENSED_STAFFING_HOURS_PER_RESIDENT_PER_DAY) AS FLOAT) AS reported_licensed_staffing_hours_per_resident_per_day,
        CAST(TRIM(REPORTED_TOTAL_NURSE_STAFFING_HOURS_PER_RESIDENT_PER_DAY) AS FLOAT) AS reported_total_nurse_staffing_hours_per_resident_per_day,
        CAST(TRIM(TOTAL_NUMBER_OF_NURSE_STAFF_HOURS_PER_RESIDENT_PER_DAY_ON_THE_WEEKEND) AS FLOAT) AS total_number_of_nurse_staff_hours_per_resident_per_day_on_the_weekend,
        CAST(TRIM(REGISTERED_NURSE_HOURS_PER_RESIDENT_PER_DAY_ON_THE_WEEKEND) AS FLOAT) AS registered_nurse_hours_per_resident_per_day_on_the_weekend,
        CAST(TRIM(REPORTED_PHYSICAL_THERAPIST_STAFFING_HOURS_PER_RESIDENT_PER_DAY) AS FLOAT) AS reported_physical_therapist_staffing_hours_per_resident_per_day,
        CAST(TRIM(TOTAL_NURSING_STAFF_TURNOVER) AS FLOAT) AS total_nursing_staff_turnover,
        CAST(TRIM(REGISTERED_NURSE_TURNOVER) AS FLOAT) AS registered_nurse_turnover,
        CAST(TRIM(NUMBER_OF_ADMINISTRATORS_WHO_HAVE_LEFT_THE_NURSING_HOME) AS FLOAT) AS number_of_administrators_who_have_left_the_nursing_home,
        CAST(TRIM(NURSING_CASE_MIX_INDEX) AS FLOAT) AS nursing_case_mix_index,
        CAST(TRIM(CASE_MIX_RN_STAFFING_HOURS_PER_RESIDENT_PER_DAY) AS FLOAT) AS case_mix_rn_staffing_hours_per_resident_per_day,
        CAST(TRIM(CASE_MIX_TOTAL_NURSE_STAFFING_HOURS_PER_RESIDENT_PER_DAY) AS FLOAT) AS case_mix_total_nurse_staffing_hours_per_resident_per_day,
        CAST(TRIM(CASE_MIX_WEEKEND_TOTAL_NURSE_STAFFING_HOURS_PER_RESIDENT_PER_DAY) AS FLOAT) AS case_mix_weekend_total_nurse_staffing_hours_per_resident_per_day,
        CAST(TRIM(NUMBER_OF_FINES) AS FLOAT) AS number_of_fines,
        CAST(TRIM(FINE_AMOUNT_IN_DOLLARS) AS FLOAT) AS fine_amount_in_dollars,
        CAST(TRIM(PERCENTAGE_OF_LONG_STAY_RESIDENTS_WHOSE_NEED_FOR_HELP_WITH_DAILY_ACTIVITIES_HAS_INCREASED) AS FLOAT) AS percentage_of_long_stay_residents_whose_need_for_help_with_daily_activities_has_increased,
        CAST(TRIM(PERCENTAGE_OF_LONG_STAY_RESIDENTS_WHO_LOSE_TOO_MUCH_WEIGHT) AS FLOAT) AS percentage_of_long_stay_residents_who_lose_too_much_weight,
        CAST(TRIM(PERCENTAGE_OF_LOW_RISK_LONG_STAY_RESIDENTS_WHO_LOSE_CONTROL_OF_THEIR_BOWELS_OR_BLADDER) AS FLOAT) AS percentage_of_low_risk_long_stay_residents_who_lose_control_of_their_bowels_or_bladder,
        CAST(TRIM(PERCENTAGE_OF_LONG_STAY_RESIDENTS_WITH_A_CATHETER_INSERTED_AND_LEFT_IN_THEIR_BLADDER) AS FLOAT) AS percentage_of_long_stay_residents_with_a_catheter_inserted_and_left_in_their_bladder,
        CAST(TRIM(PERCENTAGE_OF_LONG_STAY_RESIDENTS_WITH_A_URINARY_TRACT_INFECTION) AS FLOAT) AS percentage_of_long_stay_residents_with_a_urinary_tract_infection,
        CAST(TRIM(PERCENTAGE_OF_LONG_STAY_RESIDENTS_WHO_HAVE_DEPRESSIVE_SYMPTOMS) AS FLOAT) AS percentage_of_long_stay_residents_who_have_depressive_symptoms,
        CAST(TRIM(PERCENTAGE_OF_LONG_STAY_RESIDENTS_WHO_WERE_PHYSICALLY_RESTRAINED) AS FLOAT) AS percentage_of_long_stay_residents_who_were_physically_restrained,
        CAST(TRIM(PERCENTAGE_OF_LONG_STAY_RESIDENTS_EXPERIENCING_ONE_OR_MORE_FALLS_WITH_MAJOR_INJURY) AS FLOAT) AS percentage_of_long_stay_residents_experiencing_one_or_more_falls_with_major_injury,
        CAST(TRIM(PERCENTAGE_OF_LONG_STAY_RESIDENTS_ASSESSED_AND_APPROPRIATELY_GIVEN_THE_PNEUMOCOCCAL_VACCINE) AS FLOAT) AS percentage_of_long_stay_residents_assessed_and_appropriately_given_the_pneumococcal_vaccine,
        CAST(TRIM(PERCENTAGE_OF_LONG_STAY_RESIDENTS_WHO_RECEIVED_AN_ANTIPSYCHOTIC_MEDICATION) AS FLOAT) AS percentage_of_long_stay_residents_who_received_an_antipsychotic_medication,
        CAST(TRIM(PERCENTAGE_OF_SHORT_STAY_RESIDENTS_ASSESSED_AND_APPROPRIATELY_GIVEN_THE_PNEUMOCOCCAL_VACCINE) AS FLOAT) AS percentage_of_short_stay_residents_assessed_and_appropriately_given_the_pneumococcal_vaccine,
        CAST(TRIM(PERCENTAGE_OF_SHORT_STAY_RESIDENTS_WHO_NEWLY_RECEIVED_AN_ANTIPSYCHOTIC_MEDICATION) AS FLOAT) AS percentage_of_short_stay_residents_who_newly_received_an_antipsychotic_medication,
        CAST(TRIM(PERCENTAGE_OF_LONG_STAY_RESIDENTS_WHOSE_ABILITY_TO_MOVE_INDEPENDENTLY_WORSENED) AS FLOAT) AS percentage_of_long_stay_residents_whose_ability_to_move_independently_worsened,
        CAST(TRIM(PERCENTAGE_OF_LONG_STAY_RESIDENTS_WHO_RECEIVED_AN_ANTIANXIETY_OR_HYPNOTIC_MEDICATION) AS FLOAT) AS percentage_of_long_stay_residents_who_received_an_antianxiety_or_hypnotic_medication,
        CAST(TRIM(PERCENTAGE_OF_HIGH_RISK_LONG_STAY_RESIDENTS_WITH_PRESSURE_ULCERS) AS FLOAT) AS percentage_of_high_risk_long_stay_residents_with_pressure_ulcers,
        CAST(TRIM(PERCENTAGE_OF_LONG_STAY_RESIDENTS_ASSESSED_AND_APPROPRIATELY_GIVEN_THE_SEASONAL_INFLUENZA_VACCINE) AS FLOAT) AS percentage_of_long_stay_residents_assessed_and_appropriately_given_the_seasonal_influenza_vaccine,
        CAST(TRIM(PERCENTAGE_OF_SHORT_STAY_RESIDENTS_WHO_MADE_IMPROVEMENTS_IN_FUNCTION) AS FLOAT) AS percentage_of_short_stay_residents_who_made_improvements_in_function,
        CAST(TRIM(PERCENTAGE_OF_SHORT_STAY_RESIDENTS_WHO_WERE_ASSESSED_AND_APPROPRIATELY_GIVEN_THE_SEASONAL_INFLUENZA_VACCINE) AS FLOAT) AS percentage_of_short_stay_residents_who_were_assessed_and_appropriately_given_the_seasonal_influenza_vaccine,
        CAST(TRIM(PERCENTAGE_OF_SHORT_STAY_RESIDENTS_WHO_WERE_REHOSPITALIZED_AFTER_A_NURSING_HOME_ADMISSION) AS FLOAT) AS percentage_of_short_stay_residents_who_were_rehospitalized_after_a_nursing_home_admission,
        CAST(TRIM(PERCENTAGE_OF_SHORT_STAY_RESIDENTS_WHO_HAD_AN_OUTPATIENT_EMERGENCY_DEPARTMENT_VISIT) AS FLOAT) AS percentage_of_short_stay_residents_who_had_an_outpatient_emergency_department_visit,
        CAST(TRIM(NUMBER_OF_HOSPITALIZATIONS_PER_1000_LONG_STAY_RESIDENT_DAYS) AS FLOAT) AS number_of_hospitalizations_per_1000_long_stay_resident_days,
        CAST(TRIM(NUMBER_OF_OUTPATIENT_EMERGENCY_DEPARTMENT_VISITS_PER_1000_LONG_STAY_RESIDENT_DAYS) AS FLOAT) AS number_of_outpatient_emergency_department_visits_per_1000_long_stay_resident_days,
        row_number() OVER(PARTITION BY state_or_nation ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM
        source_data
),
-- Select the fields we want to retain and only retain rows with a rn (row_number() in previous CTE)
-- value of "1" to filter out any duplicated rows.
final AS (
    SELECT
        state_or_nation,
        cycle_1_total_number_of_health_deficiencies,
        cycle_1_total_number_of_fire_safety_deficiencies,
        cycle_2_total_number_of_health_deficiencies,
        cycle_2_total_number_of_fire_safety_deficiencies,
        cycle_3_total_number_of_health_deficiencies,
        cycle_3_total_number_of_fire_safety_deficiencies,
        average_number_of_residents_per_day,
        reported_nurse_aide_staffing_hours_per_resident_per_day,
        reported_lpn_staffing_hours_per_resident_per_day,
        reported_rn_staffing_hours_per_resident_per_day,
        reported_licensed_staffing_hours_per_resident_per_day,
        reported_total_nurse_staffing_hours_per_resident_per_day,
        total_number_of_nurse_staff_hours_per_resident_per_day_on_the_weekend,
        registered_nurse_hours_per_resident_per_day_on_the_weekend,
        reported_physical_therapist_staffing_hours_per_resident_per_day,
        total_nursing_staff_turnover,
        registered_nurse_turnover,
        number_of_administrators_who_have_left_the_nursing_home,
        nursing_case_mix_index,
        case_mix_rn_staffing_hours_per_resident_per_day,
        case_mix_total_nurse_staffing_hours_per_resident_per_day,
        case_mix_weekend_total_nurse_staffing_hours_per_resident_per_day,
        number_of_fines,
        fine_amount_in_dollars,
        percentage_of_long_stay_residents_whose_need_for_help_with_daily_activities_has_increased,
        percentage_of_long_stay_residents_who_lose_too_much_weight,
        percentage_of_low_risk_long_stay_residents_who_lose_control_of_their_bowels_or_bladder,
        percentage_of_long_stay_residents_with_a_catheter_inserted_and_left_in_their_bladder,
        percentage_of_long_stay_residents_with_a_urinary_tract_infection,
        percentage_of_long_stay_residents_who_have_depressive_symptoms,
        percentage_of_long_stay_residents_who_were_physically_restrained,
        percentage_of_long_stay_residents_experiencing_one_or_more_falls_with_major_injury,
        percentage_of_long_stay_residents_assessed_and_appropriately_given_the_pneumococcal_vaccine,
        percentage_of_long_stay_residents_who_received_an_antipsychotic_medication,
        percentage_of_short_stay_residents_assessed_and_appropriately_given_the_pneumococcal_vaccine,
        percentage_of_short_stay_residents_who_newly_received_an_antipsychotic_medication,
        percentage_of_long_stay_residents_whose_ability_to_move_independently_worsened,
        percentage_of_long_stay_residents_who_received_an_antianxiety_or_hypnotic_medication,
        percentage_of_high_risk_long_stay_residents_with_pressure_ulcers,
        percentage_of_long_stay_residents_assessed_and_appropriately_given_the_seasonal_influenza_vaccine,
        percentage_of_short_stay_residents_who_made_improvements_in_function,
        percentage_of_short_stay_residents_who_were_assessed_and_appropriately_given_the_seasonal_influenza_vaccine,
        percentage_of_short_stay_residents_who_were_rehospitalized_after_a_nursing_home_admission,
        percentage_of_short_stay_residents_who_had_an_outpatient_emergency_department_visit,
        number_of_hospitalizations_per_1000_long_stay_resident_days,
        number_of_outpatient_emergency_department_visits_per_1000_long_stay_resident_days
    FROM
        deduplicated
    WHERE
        rn = 1
)
SELECT
    -- Generate a surrogate key from natural key(s)
    state_or_nation,
    cycle_1_total_number_of_health_deficiencies,
    cycle_1_total_number_of_fire_safety_deficiencies,
    cycle_2_total_number_of_health_deficiencies,
    cycle_2_total_number_of_fire_safety_deficiencies,
    cycle_3_total_number_of_health_deficiencies,
    cycle_3_total_number_of_fire_safety_deficiencies,
    average_number_of_residents_per_day,
    reported_nurse_aide_staffing_hours_per_resident_per_day,
    reported_lpn_staffing_hours_per_resident_per_day,
    reported_rn_staffing_hours_per_resident_per_day,
    reported_licensed_staffing_hours_per_resident_per_day,
    reported_total_nurse_staffing_hours_per_resident_per_day,
    total_number_of_nurse_staff_hours_per_resident_per_day_on_the_weekend,
    registered_nurse_hours_per_resident_per_day_on_the_weekend,
    reported_physical_therapist_staffing_hours_per_resident_per_day,
    total_nursing_staff_turnover,
    registered_nurse_turnover,
    number_of_administrators_who_have_left_the_nursing_home,
    nursing_case_mix_index,
    case_mix_rn_staffing_hours_per_resident_per_day,
    case_mix_total_nurse_staffing_hours_per_resident_per_day,
    case_mix_weekend_total_nurse_staffing_hours_per_resident_per_day,
    number_of_fines,
    fine_amount_in_dollars,
    percentage_of_long_stay_residents_whose_need_for_help_with_daily_activities_has_increased,
    percentage_of_long_stay_residents_who_lose_too_much_weight,
    percentage_of_low_risk_long_stay_residents_who_lose_control_of_their_bowels_or_bladder,
    percentage_of_long_stay_residents_with_a_catheter_inserted_and_left_in_their_bladder,
    percentage_of_long_stay_residents_with_a_urinary_tract_infection,
    percentage_of_long_stay_residents_who_have_depressive_symptoms,
    percentage_of_long_stay_residents_who_were_physically_restrained,
    percentage_of_long_stay_residents_experiencing_one_or_more_falls_with_major_injury,
    percentage_of_long_stay_residents_assessed_and_appropriately_given_the_pneumococcal_vaccine,
    percentage_of_long_stay_residents_who_received_an_antipsychotic_medication,
    percentage_of_short_stay_residents_assessed_and_appropriately_given_the_pneumococcal_vaccine,
    percentage_of_short_stay_residents_who_newly_received_an_antipsychotic_medication,
    percentage_of_long_stay_residents_whose_ability_to_move_independently_worsened,
    percentage_of_long_stay_residents_who_received_an_antianxiety_or_hypnotic_medication,
    percentage_of_high_risk_long_stay_residents_with_pressure_ulcers,
    percentage_of_long_stay_residents_assessed_and_appropriately_given_the_seasonal_influenza_vaccine,
    percentage_of_short_stay_residents_who_made_improvements_in_function,
    percentage_of_short_stay_residents_who_were_assessed_and_appropriately_given_the_seasonal_influenza_vaccine,
    percentage_of_short_stay_residents_who_were_rehospitalized_after_a_nursing_home_admission,
    percentage_of_short_stay_residents_who_had_an_outpatient_emergency_department_visit,
    number_of_hospitalizations_per_1000_long_stay_resident_days,
    number_of_outpatient_emergency_department_visits_per_1000_long_stay_resident_days
FROM
    final