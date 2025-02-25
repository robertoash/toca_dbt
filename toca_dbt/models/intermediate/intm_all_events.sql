{{
    config(
        materialized = 'table',
        partition_by = {"field": "event_date", "data_type": "DATE"},
        cluster_by = ['event_name', 'product_name'],
        tags = ['hourly']
    )
}}

WITH events AS (
    SELECT
        event_date,
        event_timestamp,
        device_id,
        install_id,
        device_category,
        install_source,
        event_name,
        -- Extract each key's value correctly
        ARRAY_AGG(DISTINCT IF(param.key = 'firebase_event_origin', param.value.string_value, NULL) IGNORE NULLS)[OFFSET(0)] AS event_origin,
        ARRAY_AGG(DISTINCT IF(param.key = 'count', param.value.int_value, NULL) IGNORE NULLS)[OFFSET(0)] AS event_count,
        ARRAY_AGG(DISTINCT IF(param.key = 'product_name', {{ snake_case('param.value.string_value') }}, NULL) IGNORE NULLS)[OFFSET(0)] AS product_name,
        ARRAY_AGG(DISTINCT IF(param.key = 'price', param.value.int_value, NULL) IGNORE NULLS)[OFFSET(0)] AS price_local,
        ARRAY_AGG(DISTINCT IF(param.key = 'currency', UPPER(param.value.string_value), NULL) IGNORE NULLS)[OFFSET(0)] AS currency_code,
        ARRAY_AGG(DISTINCT IF(param.key = 'quantity', param.value.int_value, NULL) IGNORE NULLS)[OFFSET(0)] AS quantity,
        ARRAY_AGG(DISTINCT IF(param.key = 'reason', param.value.string_value, NULL) IGNORE NULLS)[OFFSET(0)] AS reason,
        ARRAY_AGG(DISTINCT IF(param.key = 'ga_dedup_id', param.value.int_value, NULL) IGNORE NULLS)[OFFSET(0)] AS ga_dedup_id,
        ARRAY_AGG(DISTINCT IF(param.key = 'ga_session_id', param.value.int_value, NULL) IGNORE NULLS)[OFFSET(0)] AS ga_session_id,
        ARRAY_AGG(DISTINCT IF(param.key = 'ga_session_number', param.value.int_value, NULL) IGNORE NULLS)[OFFSET(0)] AS ga_session_number,
        ARRAY_AGG(DISTINCT IF(param.key = 'subscription', param.value.int_value, NULL) IGNORE NULLS)[OFFSET(0)] AS subscription,
        ARRAY_AGG(DISTINCT IF(param.key = 'value', param.value.int_value, NULL) IGNORE NULLS)[OFFSET(0)] AS revenue_local
    FROM {{ ref('stg_events') }}
    CROSS JOIN UNNEST(event_params) AS param
    -- Deemed not useful for analysis
    WHERE device_id IS NOT NULL
    GROUP BY event_date, event_timestamp, event_name, device_id, install_id, device_category, install_source
)

-- Deduplicate rows
SELECT DISTINCT *
FROM events
