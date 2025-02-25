{{
    config(
        materialized = 'table',
        partition_by = {"field": "currency_exchange_date", "data_type": "DATE"},
        cluster_by = ['currency_code'],
        tags = ['daily']
    )
}}

WITH fx_data AS (
    SELECT
        currency_exchange_date,
        currency_code,
        usd_per_currency,
        LEAD(currency_exchange_date) OVER (
            PARTITION BY currency_code
            ORDER BY currency_exchange_date
        ) AS next_date
    FROM {{ ref('stg_exchange_rates') }}
),

with_end_dates AS (
    SELECT
        COALESCE(
            currency_exchange_date,
            DATE('1970-01-01')
        ) AS valid_from,
        COALESCE(
            next_date,
            DATE('2999-12-31')
        ) AS valid_to,
        currency_code,
        usd_per_currency,
        CASE
            WHEN next_date IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current
    FROM fx_data
)

SELECT
    valid_from,
    valid_to,
    currency_code,
    usd_per_currency,
    is_current
FROM with_end_dates
