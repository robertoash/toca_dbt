{{ config(
    materialized = 'table',
    partition_by = {"field": "event_date", "data_type": "DATE"},
    cluster_by = ['device_category', 'install_source'],
    tags = ['daily']
) }}

WITH telemetry_events AS (
    SELECT
        e.event_date,
        e.device_id,
        e.install_source,
        e.device_category,
        e.event_name,
        e.ga_session_id
    FROM {{ ref('intm_telemetry_events') }} AS e
),

conversion_rates AS (
    SELECT
        event_date,
        device_id,
        install_source,
        device_category,
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
    device_id,
    install_source,
    device_category,
    -- Get non-null values
    MAX(store_impression_device_id) AS store_impression_device_id,
    MAX(store_entry_device_id) AS store_entry_device_id
FROM conversion_rates
GROUP BY
    event_date,
    device_id,
    install_source,
    device_category
