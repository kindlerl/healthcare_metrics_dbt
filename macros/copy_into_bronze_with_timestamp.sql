{% macro copy_into_bronze_with_timestamp(stage, fmt, db, schema, table_name, pattern, reset=false) %}

    {% set full_table = db ~ "." ~ schema ~ "." ~ table_name %}
    {% set full_fmt = db ~ "." ~ schema ~ "." ~ fmt %}
    {%  set full_stage = db ~ "." ~ schema ~ "." ~ stage %}

    {% if reset %}
        {% do log("Reset = true, deleting from: " ~ full_table, info=True) %}
        {% set delete_stmt %}
            DELETE FROM {{ full_table }};
        {% endset %}
        {% set delete_result =  run_query(delete_stmt) %}
        {{ log(delete_result, info=true)}}
    {% endif %}

    -- Check the reset flag.  Since Snowflake tracks load history for
    -- a file, an attempt to import a file for a second time will fail
    -- unless the FORCE=TRUE flag is passed.  We only want to pass that
    -- flag if our reset flag is true.
    {% if reset %}
        {% set FORCE_FLAG = "FORCE = TRUE" %}
    {% else %}
        {% set FORCE_FLAG = "" %}
    {% endif %}

    -- Step 1: COPY INTO the Bronze table
    {% set copy_stmt %}
        COPY INTO {{ full_table }}
        FROM @{{ full_stage }}
        PATTERN = '{{ pattern }}'
        FILE_FORMAT = (FORMAT_NAME = {{ full_fmt }})
        ON_ERROR = 'CONTINUE'
        {{ FORCE_FLAG }};
    {% endset %}

    {% do log("FINAL COPY STMT:\n" ~ copy_stmt, info=True) %}
    {% set run_result = run_query(copy_stmt) %}
    {{ log(run_result, info=True) }}

    -- Step 2: Add load_timestamp if not already present
     {% do log("Altering table for: " ~ full_table, info=True) %}
     {% set alter_stmt %}
        ALTER TABLE {{ full_table }}
        ADD COLUMN IF NOT EXISTS load_timestamp TIMESTAMP_LTZ;
    {% endset %}
    {% set run_result = run_query(alter_stmt) %}
    {{ log(run_result, info=True) }}

    -- Step 3: Populate load_timestamp where NULL
     {% do log("Updating table: " ~ full_table, info=True) %}
     {% set update_stmt %}
        UPDATE {{ full_table }}
        SET load_timestamp = CURRENT_TIMESTAMP
        WHERE load_timestamp IS NULL;
    {% endset %}
    {% set run_result = run_query(update_stmt) %}
    {{ log(run_result, info=True) }}

    -- Log final results
    {% if reset %}
        {% set reset_clause = "FORCIBLY " %}
    {% else %}
        {% set reset_clause = "" %}
    {% endif %}

    {% do log("File from " ~ full_stage ~ " with pattern " ~ pattern ~ " was " ~ reset_clause + "copied into the Bronze table " ~ full_table, info=True) %}
    {% log("===================================", info=True) %}

{% endmacro %}
