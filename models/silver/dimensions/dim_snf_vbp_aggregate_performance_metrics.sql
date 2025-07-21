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
         {{ source('bronze', 'fy_2024_snf_vbp_aggregate_performance') }}
),
-- Next, trim spaces, set data types to clean the data.
final AS (
    SELECT
        CAST(TRIM(BASELINE_PERIOD_FY_2019_NATIONAL_AVERAGE_READMISSION_RATE) AS FLOAT) AS baseline_period_fy_2019_national_average_readmission_rate,
        CAST(TRIM(PERFORMANCE_PERIOD_FY_2022_NATIONAL_AVERAGE_READMISSION_RATE) AS FLOAT) AS performance_period_fy_2022_national_average_readmission_rate,
        CAST(TRIM(FY_2024_ACHIEVEMENT_THRESHOLD) AS FLOAT) AS fy_2024_achievement_threshold,
        CAST(TRIM(FY_2024_BENCHMARK) AS FLOAT) AS fy_2024_benchmark,

        TRIM(RANGE_OF_PERFORMANCE_SCORES) AS range_of_performance_scores,
        -- split range into 2 columns
        -- first column
        CASE
            WHEN REGEXP_LIKE(RANGE_OF_PERFORMANCE_SCORES, '^[0-9]\.[0-9]+\-[0-9]+\.[0-9]+$') THEN
                CAST(SPLIT_PART(TRIM(RANGE_OF_PERFORMANCE_SCORES), '-', 1) AS FLOAT)
            ELSE NULL
        END AS performance_score_range_start,
        -- second column
        CASE
            WHEN REGEXP_LIKE(RANGE_OF_PERFORMANCE_SCORES, '^[0-9]\.[0-9]+\-[0-9]+\.[0-9]+$') THEN
                CAST(SPLIT_PART(TRIM(RANGE_OF_PERFORMANCE_SCORES), '-', 2) AS FLOAT)
            ELSE NULL
        END AS performance_score_range_end,

        CAST(TRIM(TOTAL_NUMBER_OF_SNFS_RECEIVING_VALUE_BASED_INCENTIVE_PAYMENTS) AS FLOAT) AS total_number_of_snfs_receiving_value_based_incentive_payments,

        TRIM(RANGE_OF_INCENTIVE_PAYMENT_MULTIPLIERS) AS range_of_incentive_payment_multipliers,
        -- split range into 2 columns
        -- first column
        CASE
            WHEN REGEXP_LIKE(RANGE_OF_INCENTIVE_PAYMENT_MULTIPLIERS, '^[0-9]\.[0-9]+\-[0-9]+\.[0-9]+$') THEN
                CAST(SPLIT_PART(TRIM(RANGE_OF_INCENTIVE_PAYMENT_MULTIPLIERS), '-', 1) AS FLOAT)
            ELSE NULL
        END AS incentive_payment_multiplier_range_start,
        -- second column
        CASE
            WHEN REGEXP_LIKE(RANGE_OF_INCENTIVE_PAYMENT_MULTIPLIERS, '^[0-9]\.[0-9]+\-[0-9]+\.[0-9]+$') THEN
                CAST(SPLIT_PART(TRIM(RANGE_OF_INCENTIVE_PAYMENT_MULTIPLIERS), '-', 2) AS FLOAT)
            ELSE NULL
        END AS incentive_payment_multiplier_range_end,

        TRIM(RANGE_OF_VALUE_BASED_INCENTIVE_PAYMENTS) AS range_of_value_based_incentive_payments,
        -- split range into 2 columns
        -- first column
        CASE
            WHEN REGEXP_LIKE(RANGE_OF_VALUE_BASED_INCENTIVE_PAYMENTS, '^[0-9]\.[0-9]+\-[0-9]+\.[0-9]+$') THEN
                CAST(SPLIT_PART(TRIM(RANGE_OF_VALUE_BASED_INCENTIVE_PAYMENTS), '-', 1) AS FLOAT)
            ELSE NULL
        END AS value_based_incentive_payments_range_start,
        -- second column
        CASE
            WHEN REGEXP_LIKE(RANGE_OF_VALUE_BASED_INCENTIVE_PAYMENTS, '^[0-9]\.[0-9]+\-[0-9]+\.[0-9]+$') THEN
                CAST(SPLIT_PART(TRIM(RANGE_OF_VALUE_BASED_INCENTIVE_PAYMENTS), '-', 2) AS FLOAT)
            ELSE NULL
        END AS value_based_incentive_payments_range_end,

        CASE
            WHEN REGEXP_LIKE(TOTAL_AMOUNT_OF_VALUE_BASED_INCENTIVE_PAYMENTS, '^[0-9]\.[0-9]+\-[0-9]+\.[0-9]+$') THEN
                CAST(TRIM(TOTAL_AMOUNT_OF_VALUE_BASED_INCENTIVE_PAYMENTS) AS FLOAT)
            ELSE NULL
        END AS total_amount_of_value_based_incentive_payments,
    FROM
        source_data
)
SELECT
    baseline_period_fy_2019_national_average_readmission_rate,
    performance_period_fy_2022_national_average_readmission_rate,
    fy_2024_achievement_threshold,
    fy_2024_benchmark,
    range_of_performance_scores,
    performance_score_range_start,
    performance_score_range_end,
    total_number_of_snfs_receiving_value_based_incentive_payments,
    range_of_incentive_payment_multipliers,
    incentive_payment_multiplier_range_start,
    incentive_payment_multiplier_range_end,
    range_of_value_based_incentive_payments,
    value_based_incentive_payments_range_start,
    value_based_incentive_payments_range_end,
    total_amount_of_value_based_incentive_payments
FROM
    final
