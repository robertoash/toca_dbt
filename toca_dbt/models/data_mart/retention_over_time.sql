{{
    config(
        materialized = 'table',
        tags = ['daily']
    )
}}

WITH first_sessions AS (
    SELECT
        device_id,
        MIN(event_date) AS install_date
    FROM {{ ref('intm_events') }}
    GROUP BY device_id
),

retention_data AS (
    SELECT
        first_sessions.install_date,
        first_sessions.device_id,
        MIN(CASE WHEN DATE_DIFF(later_events.event_date, first_sessions.install_date, DAY) = 1 THEN 'D1' END) AS D1,
        MIN(CASE WHEN DATE_DIFF(later_events.event_date, first_sessions.install_date, DAY) BETWEEN 2 AND 3 THEN 'D3' END) AS D3,
        MIN(CASE WHEN DATE_DIFF(later_events.event_date, first_sessions.install_date, DAY) BETWEEN 4 AND 7 THEN 'D7' END) AS D7
    FROM first_sessions
    JOIN {{ ref('intm_events') }} AS later_events
        ON first_sessions.device_id = later_events.device_id  -- Ensure we track the same user
    WHERE DATE_DIFF(later_events.event_date, first_sessions.install_date, DAY) BETWEEN 1 AND 7
    GROUP BY
        first_sessions.install_date,
        first_sessions.device_id
),

retention_funnel AS (
    SELECT
        install_date,
        COUNT(DISTINCT device_id) AS d0_users,
        COUNT(DISTINCT CASE WHEN D1 IS NOT NULL THEN device_id END) AS d1_retained_users,
        COUNT(DISTINCT CASE WHEN D3 IS NOT NULL THEN device_id END) AS d3_retained_users,
        COUNT(DISTINCT CASE WHEN D7 IS NOT NULL THEN device_id END) AS d7_retained_users
    FROM retention_data
    GROUP BY install_date
)

SELECT
    install_date,
    d0_users,
    d1_retained_users,
    d3_retained_users,
    d7_retained_users,
    SAFE_DIVIDE(d1_retained_users, d0_users) AS d1_retention_rate,
    SAFE_DIVIDE(d3_retained_users, d0_users) AS d3_retention_rate,
    SAFE_DIVIDE(d7_retained_users, d0_users) AS d7_retention_rate
FROM retention_funnel
