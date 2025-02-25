{{
    config(
        materialized = 'view'
    )
}}

SELECT
    event_date,
    DATE_TRUNC(event_date, WEEK) AS event_week,
    DATE_TRUNC(event_date, MONTH) AS event_month,
    DATE_TRUNC(event_date, QUARTER) AS event_quarter,
    DATE_TRUNC(event_date, YEAR) AS event_year
FROM {{ ref('intm_all_events') }}
