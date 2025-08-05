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
         {{ source('bronze', 'nh_health_citations') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a trim() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(CMS_CERTIFICATION_NUMBER) AS provider_id,
        TO_DATE(TRIM(SURVEY_DATE), 'YYYY-MM-DD') AS survey_date,
        CAST(TRIM(SURVEY_TYPE) AS VARCHAR) AS survey_type,
        CONCAT(TRIM(DEFICIENCY_PREFIX), '-', LPAD(TRIM(DEFICIENCY_TAG_NUMBER), 4, '0')) AS deficiency_id,
        TRIM(DEFICIENCY_PREFIX) AS deficiency_prefix,
        TRIM(DEFICIENCY_TAG_NUMBER) AS deficiency_tag_number,
        TRIM(DEFICIENCY_CATEGORY) AS deficiency_category,
        TRIM(DEFICIENCY_DESCRIPTION) AS deficiency_description,
        TRIM(SCOPE_SEVERITY_CODE) AS scope_severity_code,
        TRIM(DEFICIENCY_CORRECTED) AS deficiency_corrected,
        TO_DATE(TRIM(CORRECTION_DATE), 'YYYY-MM-DD') AS correction_date,
        TRIM(INSPECTION_CYCLE) AS inspection_cycle,
        CAST(
            CASE 
                WHEN TRIM(STANDARD_DEFICIENCY) = 'Y' THEN TRUE
                WHEN TRIM(STANDARD_DEFICIENCY) = 'N' THEN FALSE
                ELSE NULL
            END AS BOOLEAN
        ) AS standard_deficiency,
        CAST(
            CASE 
                WHEN TRIM(COMPLAINT_DEFICIENCY) = 'Y' THEN TRUE
                WHEN TRIM(COMPLAINT_DEFICIENCY) = 'N' THEN FALSE
                ELSE NULL
            END AS BOOLEAN
        ) AS complaint_deficiency,
        CAST(
            CASE 
                WHEN TRIM(INFECTION_CONTROL_INSPECTION_DEFICIENCY) = 'Y' THEN TRUE
                WHEN TRIM(INFECTION_CONTROL_INSPECTION_DEFICIENCY) = 'N' THEN FALSE
                ELSE NULL
            END AS BOOLEAN
        ) AS infection_control_inspection_deficiency,
        CAST(
            CASE 
                WHEN TRIM(CITATION_UNDER_IDR) = 'Y' THEN TRUE
                WHEN TRIM(CITATION_UNDER_IDR) = 'N' THEN FALSE
                ELSE NULL
            END AS BOOLEAN
        ) AS citation_under_idr,
        CAST(
            CASE 
                WHEN TRIM(CITATION_UNDER_IIDR) = 'Y' THEN TRUE
                WHEN TRIM(CITATION_UNDER_IIDR) = 'N' THEN FALSE
                ELSE NULL
            END AS BOOLEAN
        ) AS citation_under_iidr,
        row_number() OVER(PARTITION BY provider_id, survey_date, deficiency_id ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM
        source_data
),
-- Select the fields we want to retain and only retain rows with a rn (row_number() in previous CTE)
-- value of "1" to filter out any duplicated rows.
final AS (
    SELECT
        provider_id,
        survey_date,
        survey_type,
        deficiency_id,
        deficiency_prefix,
        deficiency_tag_number,
        deficiency_category,
        deficiency_description,
        scope_severity_code,
        deficiency_corrected,
        correction_date,
        inspection_cycle,
        standard_deficiency,
        complaint_deficiency,
        infection_control_inspection_deficiency,
        citation_under_idr,
        citation_under_iidr
    FROM
        deduplicated
    WHERE
        rn = 1
)
SELECT
    -- Generate a surrogate key from natural key(s)
    {{ dbt_utils.generate_surrogate_key(['provider_id', 'survey_date', 'deficiency_id']) }} as citation_sk,
    provider_id,
    survey_date,
    survey_type,
    deficiency_id,
    deficiency_prefix,
    deficiency_tag_number,
    deficiency_category,
    deficiency_description,
    scope_severity_code,
    deficiency_corrected,
    correction_date,
    inspection_cycle,
    standard_deficiency,
    complaint_deficiency,
    infection_control_inspection_deficiency,
    citation_under_idr,
    citation_under_iidr
FROM
    final