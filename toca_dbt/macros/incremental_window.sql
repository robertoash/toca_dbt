{% macro incremental_window(timestamp_col, window_days=7) %}
LEAST(
    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ window_days }} DAY),
    (
        SELECT COALESCE(
            MAX({{ timestamp_col }}),
            TIMESTAMP('1970-01-01')
        )
        FROM {{ this }}
    )
)
{% endmacro %}