{{
    config(
        materialized = 'table',
        partition_by = {"field": "event_date", "data_type": "DATE"},
        cluster_by = ['event_name', 'product_name'],
        tags = ['hourly']
    )
}}

WITH telemetry_events AS (
    SELECT
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
        ga_dedup_id,
        ga_session_id,
        ga_session_number,
        subscription,
        revenue_local
    FROM {{ ref('intm_all_events') }}
    WHERE event_name != 'in_app_purchase'
)

SELECT DISTINCT *
FROM telemetry_events
