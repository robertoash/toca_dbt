{{ config(
    materialized = 'table',
    partition_by = {"field": "event_date", "data_type": "DATE"},
    cluster_by = ['event_name', 'device_category'],
    tags = ['daily']
) }}

WITH telemetry_events AS (
    SELECT
        event_date,
        device_id,
        install_source,
        device_category,
        event_name,
        event_origin,
        event_count,
        ga_dedup_id,
        ga_session_id,
        ga_session_number,
        store_impression_device_id,
        store_entry_device_id
    FROM {{ ref('intm_telemetry_events') }}
),

aggregated_events AS (
    SELECT
        event_date,
        device_id,
        install_source,
        device_category,
        event_name,
        COUNT(*) AS event_count,
        COUNT(DISTINCT ga_session_id) AS session_count,
        COUNT(DISTINCT store_impression_device_id) AS store_impression_count,
        COUNT(DISTINCT store_entry_device_id) AS store_entry_count
    FROM telemetry_events
    GROUP BY
        event_date,
        device_id,
        install_source,
        device_category,
        event_name
)

SELECT *
FROM aggregated_events
