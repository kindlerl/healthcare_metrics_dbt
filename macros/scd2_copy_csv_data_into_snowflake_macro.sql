{% macro scd2_copy_csv_data_into_snowflake_macro(table_nm) %}
 
    delete from {{var ('rawhist_db') }}.{{var ('wrk_schema')}}.{{ table_nm }};

    {% do log("Running COPY INTO from stage: " ~ var('stage_name'), info=True) %}

    COPY INTO {{var ('rawhist_db') }}.{{var ('wrk_schema')}}.{{ table_nm }} 
    FROM (
        SELECT
            $1 AS ProductId,
            $2 AS ProductName,
            $3 AS Category,
            $4 AS SellingPrice,
            $5 AS ModelNumber,
            $6 AS AboutProduct,
            $7 AS ProductSpecification,
            $8 AS TechnicalDetails,
            $9 AS ShippingWeight,
            $10 AS ProductDimensions,
            CURRENT_TIMESTAMP() AS INSERT_DTS,
            CURRENT_TIMESTAMP() AS UPDATE_DTS,
            metadata$filename AS SOURCE_FILE_NAME,
            metadata$file_row_number AS SOURCE_FILE_ROW_NUMBER
        FROM @{{ var('stage_name') }}
    )
    FILE_FORMAT = {{var ('file_format_csv') }}
    PURGE = {{ var('purge_status') }}
    FORCE = TRUE;
 
{% endmacro %}
