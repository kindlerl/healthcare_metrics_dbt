{{ 
    config(
        database='HEALTHCARE_DB',
        schema='SILVER',
        materialized='table',
        tags=['silver', 'healthcare']
    )
}}

WITH parsed AS (
    SELECT
        STATE AS state,
        
        -- 5-Star
        CASE
            WHEN POSITION('-', FIVE_STARS) > 0 THEN SPLIT_PART(FIVE_STARS, '-', 1)::FLOAT
            ELSE NULL
        END AS five_star_min,

        CASE 
            WHEN POSITION('-', FIVE_STARS) > 0 THEN SPLIT_PART(FIVE_STARS, '-', 2)::FLOAT
            ELSE NULL 
        END AS five_star_max,

        -- 4-Star
        SPLIT_PART(FOUR_STARS, '-', 1)::FLOAT AS four_star_min,
        CASE 
            WHEN POSITION('-', FOUR_STARS) > 0 THEN SPLIT_PART(FOUR_STARS, '-', 2)::FLOAT
            ELSE NULL 
        END AS four_star_max,

        -- 3-Star
        SPLIT_PART(THREE_STARS, '-', 1)::FLOAT AS three_star_min,
        CASE 
            WHEN POSITION('-', THREE_STARS) > 0 THEN SPLIT_PART(THREE_STARS, '-', 2)::FLOAT
            ELSE NULL 
        END AS three_star_max,

        -- 2-Star
        SPLIT_PART(TWO_STARS, '-', 1)::FLOAT AS two_star_min,
        CASE 
            WHEN POSITION('-', TWO_STARS) > 0 THEN SPLIT_PART(TWO_STARS, '-', 2)::FLOAT
            ELSE NULL 
        END AS two_star_max,

        -- 1-Star (special case: ">105.583")
        CASE 
            WHEN ONE_STAR LIKE '>%' THEN REPLACE(ONE_STAR, '>', '')::FLOAT
            ELSE NULL 
        END AS one_star_min

    FROM 
        {{ source('bronze', 'nh_health_inspec_cutpoints_state') }}
    WHERE 
        STATE IS NOT NULL
)

SELECT * FROM parsed

