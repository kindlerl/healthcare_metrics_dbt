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
         {{ source('bronze', 'fy_2024_snf_vbp_facility_performance') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a trim() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(CMS_CERTIFICATION_NUMBER) AS provider_id,
        CAST(TRIM(SNF_VBP_PROGRAM_RANKING) AS INT) AS snf_vbp_program_ranking,
        TRIM(FOOTNOTE_SNF_VBP_PROGRAM_RANKING) AS footnote_snf_vbp_program_ranking,
        CASE
            WHEN TRIM(BASELINE_PERIOD_FY_2019_RISK_STANDARDIZED_READMISSION_RATE) LIKE '--%' THEN NULL
            ELSE CAST(TRIM(BASELINE_PERIOD_FY_2019_RISK_STANDARDIZED_READMISSION_RATE) AS FLOAT)
        END AS baseline_period_fy_2019_risk_standardized_readmission_rate,
        TRIM(FOOTNOTE_BASELINE_PERIOD_FY_2019_RISK_STANDARDIZED_READMISSION_RATE) AS footnote_baseline_period_fy_2019_risk_standardized_readmission_rate,
        CASE
            WHEN TRIM(PERFORMANCE_PERIOD_FY_2022_RISK_STANDARDIZED_READMISSION_RATE) LIKE '--%' THEN NULL
            ELSE CAST(TRIM(PERFORMANCE_PERIOD_FY_2022_RISK_STANDARDIZED_READMISSION_RATE) AS FLOAT)
        END AS performance_period_fy_2022_risk_standardized_readmission_rate,
        TRIM(FOOTNOTE_PERFORMANCE_PERIOD_FY_2022_RISK_STANDARDIZED_READMISSION_RATE) AS footnote_performance_period_fy_2022_risk_standardized_readmission_rate,
        CAST(TRIM(ACHIEVEMENT_SCORE) AS FLOAT) AS achievement_score,
        TRIM(FOOTNOTE_ACHIEVEMENT_SCORE) AS footnote_achievement_score,
        CASE
            WHEN TRIM(IMPROVEMENT_SCORE) LIKE '--%' THEN NULL
            ELSE CAST(TRIM(IMPROVEMENT_SCORE) AS FLOAT)
        END AS improvement_score,
        TRIM(FOOTNOTE_IMPROVEMENT_SCORE) AS footnote_improvement_score,
        CAST(TRIM(PERFORMANCE_SCORE) AS FLOAT) AS performance_score,
        TRIM(FOOTNOTE_PERFORMANCE_SCORE) footnote_performance_score,
        CAST(TRIM(INCENTIVE_PAYMENT_MULTIPLIER) AS FLOAT) AS incentive_payment_multiplier,
        TRIM(FOOTNOTE_INCENTIVE_PAYMENT_MULTIPLIER) AS footnote_incentive_payment_multiplier,
        row_number() OVER(PARTITION BY provider_id ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM
        source_data
),
-- Select the fields we want to retain and only retain rows with a rn (row_number() in previous CTE)
-- value of "1" to filter out any duplicated rows.
final AS (
    SELECT
        provider_id,
        snf_vbp_program_ranking,
        footnote_snf_vbp_program_ranking,
        baseline_period_fy_2019_risk_standardized_readmission_rate,
        footnote_baseline_period_fy_2019_risk_standardized_readmission_rate,
        performance_period_fy_2022_risk_standardized_readmission_rate,
        footnote_performance_period_fy_2022_risk_standardized_readmission_rate,
        achievement_score,
        footnote_achievement_score,
        improvement_score,
        footnote_improvement_score,
        performance_score,
        footnote_performance_score,
        incentive_payment_multiplier,
        footnote_incentive_payment_multiplier
    FROM
        deduplicated
    WHERE
        rn = 1
)
SELECT
        provider_id,
        snf_vbp_program_ranking,
        footnote_snf_vbp_program_ranking,
        baseline_period_fy_2019_risk_standardized_readmission_rate,
        footnote_baseline_period_fy_2019_risk_standardized_readmission_rate,
        performance_period_fy_2022_risk_standardized_readmission_rate,
        footnote_performance_period_fy_2022_risk_standardized_readmission_rate,
        achievement_score,
        footnote_achievement_score,
        improvement_score,
        footnote_improvement_score,
        performance_score,
        footnote_performance_score,
        incentive_payment_multiplier,
        footnote_incentive_payment_multiplier
FROM
    final