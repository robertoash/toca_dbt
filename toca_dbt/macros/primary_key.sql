{% macro primary_key_gen(columns) %}
    md5(CONCAT({{ columns | map('trim') | map('string') | join(",'||',") }}))
{% endmacro %}
