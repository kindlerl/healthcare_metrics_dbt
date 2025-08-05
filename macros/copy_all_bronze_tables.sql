{% macro copy_all_bronze_tables() %}
    {%  set config = var('bronze_copy_config')  %}
    {%  set database = config.database          %}
    {%  set schema = config.schema              %}
    {%  set stage = config.stage_name           %}
    {%  set fmt = config.format_name            %}
    {%  set full_stage = database ~ "." ~ schema ~ "." ~ stage %}

    
    {{ log("LISTING ALL FILES IN STAGE: " ~ full_stage, info=True) }}
    {% set list_stmt %}
        LIST @{{ full_stage }};
    {% endset %}
    {% set list_result = run_query(list_stmt) %}
    {{ log("All files in stage: " ~ list_result, info=true) }}

    {% for entry in config.tables %}
        {% set reset_flag = entry.reset if entry.reset is not none else false %}
        {% do log("Running COPY INTO for: " ~ database ~ "." ~ schema ~ "." ~ entry.table_name ~ " with reset=" ~ reset_flag, info=True) %}
        {{ copy_into_bronze_with_timestamp(stage, fmt, database, schema, entry.table_name, entry.pattern, reset_flag) }}
    {% endfor %}

{% endmacro %}
