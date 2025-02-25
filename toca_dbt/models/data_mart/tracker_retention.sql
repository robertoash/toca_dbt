{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'device_retention_id',
        partition_by = {"field": "first_active_date", "data_type": "DATE"},
        cluster_by = ['retention_tier'],
        tags = ['daily']
    )
}}

/*
    Retention is here defined as generating any event within a given period after
    generating a first event. The period can be 1, 3 or 7 days.
*/

WITH event_data AS (
    SELECT
        device_id,
        DATE_DIFF(event_date, MIN(event_date) OVER (PARTITION BY device_id), DAY) AS retention_days,
        MIN(event_date) OVER (PARTITION BY device_id) AS first_active_date
    FROM {{ ref('intm_all_events') }}
    {% if is_incremental() %}
        WHERE event_date >= {{ incremental_window(event_date, 2) }}
    {% endif %}
    GROUP BY
        device_id,
        event_date
),

retention_tiers AS (
    -- D0 tier (all users)
    SELECT DISTINCT
        device_id,
        first_active_date,
        'D0' AS retention_tier
    FROM event_data

    UNION ALL

    -- D1 tier
    SELECT DISTINCT
        device_id,
        first_active_date,
        'D1' AS retention_tier
    FROM event_data
    WHERE retention_days = 1

    UNION ALL

    -- D3 tier
    SELECT DISTINCT
        device_id,
        first_active_date,
        'D3' AS retention_tier
    FROM event_data
    WHERE retention_days BETWEEN 2 AND 3

    UNION ALL

    -- D7 tier
    SELECT DISTINCT
        device_id,
        first_active_date,
        'D7' AS retention_tier
    FROM event_data
    WHERE retention_days BETWEEN 4 AND 7
)

SELECT
    {{
        dbt_utils.generate_surrogate_key(
            ['device_id',
            'retention_tier']
        )
    }} AS device_retention_id,
    first_active_date,
    device_id, -- This is meant to be count distincted
    retention_tier
FROM retention_tiers
