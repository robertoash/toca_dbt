{{
    config(
        materialized = 'table',
        partition_by = {"field": "valid_from", "data_type": "DATE"},
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
        ) AS next_date,
        LAG(currency_exchange_date) OVER (
            PARTITION BY currency_code
            ORDER BY currency_exchange_date
        ) AS prev_date
    FROM {{ ref('stg_exchange_rates') }}
),

with_end_dates AS (
    SELECT
        IF(
            prev_date IS NULL,
            DATE('1970-01-01'),
            currency_exchange_date
        ) AS valid_from,
        COALESCE(
            DATE_SUB(next_date, INTERVAL 1 DAY),
            DATE('2999-12-31')
        ) AS valid_to,
        currency_code,
        usd_per_currency,
        IF(next_date IS NULL, TRUE, FALSE) AS is_current
    FROM fx_data
)

SELECT
    valid_from,
    valid_to,
    currency_code,
    usd_per_currency,
    is_current
FROM with_end_dates
