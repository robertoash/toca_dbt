{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'event_id',
        partition_by = {"field": "event_date", "data_type": "DATE"},
        cluster_by = ['event_name', 'product_name'],
        tags = ['hourly']
    )
}}

WITH events AS (
    SELECT
        event_id,
        event_date,
        event_timestamp,
        device_id,
        install_id,
        device_category,
        install_source,
        event_name,
        -- ARRAY_AGG to handle multiple values for each key but we take only the first
        ARRAY_AGG(DISTINCT IF(
            params.key = 'firebase_event_origin',
            params.value.string_value,
            NULL
        ) IGNORE NULLS)[OFFSET(0)] AS event_origin,
        ARRAY_AGG(DISTINCT IF(
            params.key = 'count',
            params.value.int_value,
            NULL
        ) IGNORE NULLS)[OFFSET(0)] AS event_count,
        ARRAY_AGG(DISTINCT IF(
            params.key = 'product_name',
            {{ snake_case('params.value.string_value') }},
            NULL
        ) IGNORE NULLS)[OFFSET(0)] AS product_name,
        ARRAY_AGG(DISTINCT IF(
            params.key = 'price',
            params.value.int_value,
            NULL
        ) IGNORE NULLS)[OFFSET(0)] AS price_local,
        ARRAY_AGG(DISTINCT IF(
            params.key = 'currency',
            UPPER(params.value.string_value),
            NULL
        ) IGNORE NULLS)[OFFSET(0)] AS currency_code,
        ARRAY_AGG(DISTINCT IF(
            params.key = 'quantity',
            params.value.int_value,
            NULL
        ) IGNORE NULLS)[OFFSET(0)] AS quantity,
        ARRAY_AGG(DISTINCT IF(
            params.key = 'reason',
            params.value.string_value,
            NULL
        ) IGNORE NULLS)[OFFSET(0)] AS reason,
        ARRAY_AGG(DISTINCT IF(
            params.key = 'ga_dedup_id',
            params.value.int_value,
            NULL
        ) IGNORE NULLS)[OFFSET(0)] AS ga_dedup_id,
        ARRAY_AGG(DISTINCT IF(
            params.key = 'ga_session_id',
            params.value.int_value,
            NULL
        ) IGNORE NULLS)[OFFSET(0)] AS ga_session_id,
        ARRAY_AGG(DISTINCT IF(
            params.key = 'ga_session_number',
            params.value.int_value,
            NULL
        ) IGNORE NULLS)[OFFSET(0)] AS ga_session_number,
        ARRAY_AGG(DISTINCT IF(
            params.key = 'subscription',
            params.value.int_value,
            NULL
        ) IGNORE NULLS)[OFFSET(0)] AS subscription,
        ARRAY_AGG(DISTINCT IF(
            params.key = 'value',
            params.value.int_value,
            NULL
        ) IGNORE NULLS)[OFFSET(0)] AS revenue_local
    FROM {{ ref('stg_events') }}
    CROSS JOIN UNNEST(event_params) AS params
    -- Deemed not useful for analysis
    WHERE device_id IS NOT NULL
    {% if is_incremental() %}
        -- Pull the last 7 days of data to account for event loading delays
        AND event_timestamp >= {{ incremental_window('event_timestamp', 2) }}
    {% endif %}
    GROUP BY
        event_id,
        event_date,
        event_timestamp,
        event_name,
        device_id,
        install_id,
        device_category,
        install_source
)

SELECT *
FROM events
