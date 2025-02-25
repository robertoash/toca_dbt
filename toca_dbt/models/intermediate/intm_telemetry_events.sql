{{
    config(
        materialized = 'table',
        partition_by = {"field": "event_date", "data_type": "DATE"},
        cluster_by = ['event_name', 'device_id'],
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
        ga_dedup_id,
        ga_session_id,
        ga_session_number,
    FROM {{ ref('intm_all_events') }}
    WHERE
        event_name != 'in_app_purchase'
),

with_store_events AS (
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
        ga_dedup_id,
        ga_session_id,
        ga_session_number,
        CASE
            WHEN event_name = 'store_impression' THEN device_id
            ELSE NULL
        END AS store_impression_device_id,
        CASE
            WHEN event_name = 'store_entry' THEN device_id
            ELSE NULL
        END AS store_entry_device_id
    FROM telemetry_events
)

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
    ga_dedup_id,
    ga_session_id,
    ga_session_number,
    MAX(store_impression_device_id) AS store_impression_device_id,
    MAX(store_entry_device_id) AS store_entry_device_id
FROM with_store_events
GROUP BY
    event_date,
    event_timestamp,
    device_id,
    install_id,
    device_category,
    install_source,
    event_name,
    event_origin,
    event_count,
    ga_dedup_id,
    ga_session_id,
    ga_session_number
