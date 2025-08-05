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
         {{ source('bronze', 'nh_ownership') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a trim() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
--
-- NOTE:    The source data was missing 543 owner_name values (0.037%).  Since the 
--          owner_name is critical to the identification of the row, the data in the
--          row becomes unusable if it is missing.  Therefore, drop the rows that 
--          have no owner_name value.
deduplicated AS (
    SELECT
        TRIM(CMS_CERTIFICATION_NUMBER) AS provider_id,
        TRIM(OWNER_NAME) AS owner_name,
        TRIM(ROLE_PLAYED_BY_OWNER_OR_MANAGER_IN_FACILITY) AS owner_role,
        TRIM(OWNER_TYPE) AS ownership_type,
        CASE
            WHEN OWNERSHIP_PERCENTAGE IN ('NOT APPLICABLE', 'NO PERCENTAGE PROVIDED') THEN NULL
            ELSE CAST(REPLACE(TRIM(OWNERSHIP_PERCENTAGE), '%', '') AS FLOAT)
        END AS ownership_percentage,
        CASE
            WHEN ASSOCIATION_DATE LIKE 'since%' THEN CAST(TRIM(SUBSTR(ASSOCIATION_DATE,6)) AS DATE)
            WHEN ASSOCIATION_DATE LIKE 'NO DATE PROVIDED' THEN NULL
            ELSE CAST(TRIM(ASSOCIATION_DATE) AS DATE)
        END AS owner_start_date,
        ROW_NUMBER() OVER(PARTITION BY provider_id, owner_name, owner_role ORDER BY LOAD_TIMESTAMP DESC NULLS LAST) AS rn
    FROM
        source_data
    WHERE
        CMS_CERTIFICATION_NUMBER IS NOT NULL
    AND
        OWNER_NAME IS NOT NULL
    AND
        ROLE_PLAYED_BY_OWNER_OR_MANAGER_IN_FACILITY IS NOT NULL
),
final AS (
    SELECT
        provider_id,
        owner_name,
        owner_role,
        ownership_type,
        ownership_percentage,
        owner_start_date
    FROM
        deduplicated
    WHERE
        rn = 1
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['provider_id', 'owner_name', 'owner_role']) }} as provider_sk,
    provider_id,
    owner_name,
    owner_role,
    ownership_type,
    ownership_percentage,
    owner_start_date
FROM
    final
