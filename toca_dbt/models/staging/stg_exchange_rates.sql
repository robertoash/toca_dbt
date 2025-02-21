{{
    config(
        materialized = 'view'
    )
}}

WITH exchange_rates AS (
    SELECT
        dt AS currency_exchange_date,
        currency_code,
        usd_per_currency,
        is_extrapolated
    FROM {{ source('ae_assignment_data', 'exchange_rates') }}
)

SELECT * FROM exchange_rates
