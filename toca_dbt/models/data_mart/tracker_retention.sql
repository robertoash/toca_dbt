{{
    config(
        materialized = 'table',
        partition_by = {"field": "install_date", "data_type": "DATE"},
        tags = ['daily']
    )
}}

/*
    Retention is here defined as generating any telemetry event within a given period after
    generating a first event. The period can be 1, 3 or 7 days.
*/

WITH event_data AS (
    SELECT
        device_id,
        DATE_DIFF(event_date, MIN(event_date) OVER (PARTITION BY device_id), DAY) AS retention_days,
        MIN(event_date) OVER (PARTITION BY device_id) AS install_date
    FROM {{ ref('intm_all_events') }}
    GROUP BY
        device_id,
        event_date
),

retention_tiers AS (
    -- D0 tier (all users)
    SELECT DISTINCT
        device_id,
        install_date,
        'D0' AS retention_tier
    FROM event_data

    UNION ALL

    -- D1 tier
    SELECT DISTINCT
        device_id,
        install_date,
        'D1' AS retention_tier
    FROM event_data
    WHERE retention_days = 1

    UNION ALL

    -- D3 tier
    SELECT DISTINCT
        device_id,
        install_date,
        'D3' AS retention_tier
    FROM event_data
    WHERE retention_days BETWEEN 2 AND 3

    UNION ALL

    -- D7 tier
    SELECT DISTINCT
        device_id,
        install_date,
        'D7' AS retention_tier
    FROM event_data
    WHERE retention_days BETWEEN 4 AND 7
)

SELECT
    install_date,
    device_id,
    retention_tier
FROM retention_tiers
