{{
    config(
        materialized = 'view'
    )
}}

WITH exchange_rates AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['dt', 'currency_code']) }} AS exchange_rate_id,
        dt AS currency_exchange_date,
        currency_code,
        usd_per_currency,
        is_extrapolated
    FROM {{ source('ae_assignment_data', 'exchange_rates') }}
)

SELECT * FROM exchange_rates
