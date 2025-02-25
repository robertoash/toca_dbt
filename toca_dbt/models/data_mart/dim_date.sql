{{
    config(
        materialized = 'view'
    )
}}

SELECT
    event_date AS date,
    DATE_TRUNC(event_date, WEEK) AS date_week,
    DATE_TRUNC(event_date, MONTH) AS date_month,
    DATE_TRUNC(event_date, QUARTER) AS date_quarter,
    DATE_TRUNC(event_date, YEAR) AS date_year
FROM {{ ref('intm_events') }}
