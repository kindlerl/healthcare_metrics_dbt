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
         {{ source('bronze', 'pbj_daily_nurse_staffing_main') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a trim() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(PROVNUM) AS provider_id,
        CAST(TRIM(WORKDATE) AS date) AS staffing_date,
        CAST(TRIM(MDSCENSUS) AS int) AS mds_census,
        CAST(TRIM(HRS_RNDON) AS float) AS hrs_rn_don,
        CAST(TRIM(HRS_RNDON_EMP) AS float) AS hrs_rn_don_emp,
        CAST(TRIM(HRS_RNDON_CTR) AS float) AS hrs_rn_don_ctr,
        CAST(TRIM(HRS_RNADMIN) AS float) AS hrs_rn_admin,
        CAST(TRIM(HRS_RNADMIN_EMP) AS float) AS hrs_rn_admin_emp,
        CAST(TRIM(HRS_RNADMIN_CTR) AS float) AS hrs_rn_admin_ctr,
        CAST(TRIM(HRS_RN) AS float) AS hrs_rn,
        CAST(TRIM(HRS_RN_EMP) AS float) AS hrs_rn_emp,
        CAST(TRIM(HRS_RN_CTR) AS float) AS hrs_rn_ctr,
        CAST(TRIM(HRS_LPNADMIN) AS float) AS hrs_lpn_admin,
        CAST(TRIM(HRS_LPNADMIN_EMP) AS float) AS hrs_lpn_admin_emp,
        CAST(TRIM(HRS_LPNADMIN_CTR) AS float) AS hrs_lpn_admin_ctr,
        CAST(TRIM(HRS_LPN) AS float) AS hrs_lpn,
        CAST(TRIM(HRS_LPN_EMP) AS float) AS hrs_lpn_emp,
        CAST(TRIM(HRS_LPN_CTR) AS float) AS hrs_lpn_ctr,
        CAST(TRIM(HRS_CNA) AS float) AS hrs_cna,
        CAST(TRIM(HRS_CNA_EMP) AS float) AS hrs_cna_emp,
        CAST(TRIM(HRS_CNA_CTR) AS float) AS hrs_cna_ctr,
        CAST(TRIM(HRS_NATRN) AS float) AS hrs_nat_rn,
        CAST(TRIM(HRS_NATRN_EMP) AS float) AS hrs_nat_rn_emp,
        CAST(TRIM(HRS_NATRN_CTR) AS float) AS hrs_nat_rn_ctr,
        CAST(TRIM(HRS_MEDAIDE) AS float) AS hrs_med_aide,
        CAST(TRIM(HRS_MEDAIDE_EMP) AS float) AS hrs_med_aide_emp,
        CAST(TRIM(HRS_MEDAIDE_CTR) AS float) AS hrs_med_aide_ctr,
        row_number() OVER(PARTITION BY provider_id, staffing_date ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM
        source_data
),
-- Select the fields we want to retain and only retain rows with a rn (row_number() in previous CTE)
-- value of "1" to filter out any duplicated rows.
final AS (
    SELECT
        provider_id,
        staffing_date,
        mds_census,
        hrs_rn_don,
        hrs_rn_don_emp,
        hrs_rn_don_ctr,
        hrs_rn_admin,
        hrs_rn_admin_emp,
        hrs_rn_admin_ctr,
        hrs_rn,
        hrs_rn_emp,
        hrs_rn_ctr,
        hrs_lpn_admin,
        hrs_lpn_admin_emp,
        hrs_lpn_admin_ctr,
        hrs_lpn,
        hrs_lpn_emp,
        hrs_lpn_ctr,
        hrs_cna,
        hrs_cna_emp,
        hrs_cna_ctr,
        hrs_nat_rn,
        hrs_nat_rn_emp,
        hrs_nat_rn_ctr,
        hrs_med_aide,
        hrs_med_aide_emp,
        hrs_med_aide_ctr
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
    -- Generate a surrogate key from natural key(s)
    {{ dbt_utils.generate_surrogate_key(['provider_id', 'staffing_date']) }} as provider_daily_staffing_sk,
    *
FROM
    final