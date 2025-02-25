{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id',
        partition_by={
            'field': 'event_date',
            'data_type': 'date'
        },
        cluster_by=['product_name'],
        tags=['hourly']
    )
}}

WITH purchase_events AS (
    SELECT
        event_id,
        event_date,
        event_timestamp,
        device_id,
        install_id,
        device_category,
        install_source,
        event_name,
        event_origin,
        event_count,
        product_name,
        price_local,
        currency_code,
        quantity,
        reason,
        subscription,
        revenue_local
    FROM {{ ref('intm_all_events') }}
    WHERE event_name = 'in_app_purchase'
    {% if is_incremental() %}
        -- Pull the last 7 days of data to account for event loading delays
        AND event_timestamp >= {{ incremental_window('event_timestamp', 2) }}
    {% endif %}
)

SELECT *
FROM purchase_events
