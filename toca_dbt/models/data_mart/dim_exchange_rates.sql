{{
    config(
        materialized = 'view',
    )
}}

SELECT
    currency_exchange_date,
    currency_code,
    usd_per_currency
FROM {{ ref('stg_exchange_rates') }}
