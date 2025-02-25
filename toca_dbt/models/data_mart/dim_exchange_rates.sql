{{
    config(
        materialized = 'view',
    )
}}

WITH fx_data AS (
    SELECT
        currency_exchange_date,
        currency_code,
        usd_per_currency
    FROM {{ ref('stg_exchange_rates') }}
),

all_dates AS (
    SELECT date_day AS currency_exchange_date
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            (SELECT MIN(currency_exchange_date) FROM fx_data),
            CURRENT_DATE()
        )
    ) AS date_day
),

dates_currencies AS (
    SELECT DISTINCT
        ds.currency_exchange_date,
        ccs.currency_code
    FROM all_dates AS ds
    CROSS JOIN (
        SELECT DISTINCT currency_code
        FROM fx_data
    ) AS ccs
),

latest_rates AS (
    SELECT
        dc.currency_exchange_date,
        dc.currency_code,
        FIRST_VALUE(er.usd_per_currency) OVER (
            PARTITION BY dc.currency_code
            ORDER BY er.currency_exchange_date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS usd_per_currency
    FROM dates_currencies dc
    LEFT JOIN fx_data AS er
        ON dc.currency_code = er.currency_code
        AND er.currency_exchange_date <= dc.currency_exchange_date
)

SELECT
    currency_exchange_date,
    currency_code,
    usd_per_currency
FROM latest_rates
WHERE usd_per_currency IS NOT NULL
