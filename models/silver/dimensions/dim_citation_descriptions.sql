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
         {{ source('bronze', 'nh_citation_descriptions') }}
),
-- Next, try to remove duplicates.  This is achieved by selecting the fields
-- we want to retain, applying a trim() function to them to remove leading/traling
-- spacces, then applying a "row_number()" aggregator function to apply a 
-- numeric row number to each unique row.  Duplicate rows will have a row number
-- greater than 1, so we can filter those out in the next CTE.
deduplicated AS (
    SELECT
        TRIM(DEFICIENCY_PREFIX_AND_NUMBER) AS deficiency_id,
        TRIM(DEFICIENCY_PREFIX) AS deficiency_prefix,
        TRIM(DEFICIENCY_TAG_NUMBER) AS deficiency_tag_number,
        TRIM(DEFICIENCY_DESCRIPTION) AS deficiency_description,
        TRIM(DEFICIENCY_CATEGORY) AS deficiency_category,
        ROW_NUMBER() OVER(PARTITION BY deficiency_id ORDER BY LOAD_TIMESTAMP DESC NULLS LAST) AS rn
    FROM
        source_data
),
final AS (
    SELECT
        deficiency_id,
        deficiency_prefix,
        deficiency_tag_number,
        deficiency_description,
        deficiency_category
    FROM
        deduplicated
    WHERE
        rn = 1
    AND
        deficiency_id IS NOT NULL
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['deficiency_id']) }} as citation_sk,
    deficiency_id,
    deficiency_prefix,
    deficiency_tag_number,
    deficiency_description,
    deficiency_category
FROM
    final
