{{ config(
    materialized = 'table',
    partition_by = {"field": "event_date", "data_type": "DATE"},
    cluster_by = ['device_category', 'install_source'],
    tags = ['daily']
) }}

WITH past_purchases AS (
    -- Identify all players who have made a purchase before a given event date
    SELECT
        device_id,
        MAX(event_date) AS last_purchase_date
    FROM {{ ref('intm_events') }}
    WHERE event_name = 'in_app_purchase'
    GROUP BY device_id
),

store_funnel AS (
    SELECT
        e.event_date,
        e.device_category,
        e.install_source,
        COUNT(DISTINCT CASE WHEN e.event_name = 'store_impression' THEN e.ga_session_id END) AS store_impressions,
        COUNT(DISTINCT CASE WHEN e.event_name = 'store_entry' THEN e.ga_session_id END) AS store_entries,
        COUNT(DISTINCT CASE WHEN e.event_name = 'session_start' THEN e.ga_session_id END) AS unique_sessions,
        COUNT(DISTINCT CASE
            WHEN e.event_name = 'store_entry'
            AND p.device_id IS NOT NULL
            AND e.event_date > p.last_purchase_date
            THEN e.device_id
        END) AS recurring_store_entries
    FROM {{ ref('intm_events') }} AS e
    LEFT JOIN past_purchases AS p ON e.device_id = p.device_id
    GROUP BY e.event_date
),

conversion_rates AS (
    SELECT
        event_date,
        device_category,
        install_source,
        store_impressions,
        store_entries,
        unique_sessions,
        recurring_store_entries,
        SAFE_DIVIDE(store_entries, unique_sessions) AS store_entry_conversion_rate
    FROM store_funnel
)

SELECT *
FROM conversion_rates
