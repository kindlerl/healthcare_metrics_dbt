{% macro concat_macro(v1, v2) %}
    concat( '{{v1}}', '-', '{{v2}}' )
{% endmacro %}