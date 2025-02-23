{% macro snake_case(column_name) %}
    LOWER(TRIM(REGEXP_REPLACE({{ column_name }}, r'[^a-zA-Z0-9]+', '_')))
{% endmacro %}