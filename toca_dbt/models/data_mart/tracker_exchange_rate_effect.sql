{{
    config(
        materialized = 'table',
        partition_by = {"field": "event_date", "data_type": "DATE"},
        cluster_by = ['product_name', 'currency_code'],
        tags = ['daily']
    )
}}

WITH exchange_rates AS (
    SELECT
        currency_exchange_date,
        currency_code,
        usd_per_currency AS exchange_rate
    FROM `ra_ae_assignment.dim_exchange_rates`
),

purchases AS (
    SELECT
        event_date,
        product_name,
        currency_code,
        revenue_usd,
        SUM(quantity) AS quantity
    FROM {{ ref('fact_purchases') }}
    GROUP BY
        event_date,
        product_name,
        currency_code,
        revenue_usd
),

product_usd_prices AS (
    -- Get the average revenue in USD for each product when purchased in USD
    SELECT
        event_date,
        product_name,
        AVG(revenue_usd) AS avg_usd_revenue
    FROM purchases
    WHERE currency_code = 'USD'
    GROUP BY
        event_date,
        product_name
),

product_other_prices AS (
    -- Get the average revenue in USD for each product when purchased in USD
    SELECT
        event_date,
        product_name,
        currency_code,
        AVG(revenue_usd) AS avg_usd_revenue,
        SUM(quantity) AS quantity
    FROM purchases
    WHERE currency_code != 'USD'
    GROUP BY
        event_date,
        product_name,
        currency_code
),

currency_deviation AS (
    -- Compare the average revenue in USD for the same products sold in other currencies
    SELECT
        pu.event_date,
        pu.product_name,
        po.currency_code,
        xr.exchange_rate,
        pu.avg_usd_revenue AS avg_revenue_usd,
        po.avg_usd_revenue AS avg_revenue_usd_incl_xr,
        SAFE_DIVIDE(po.avg_usd_revenue, pu.avg_usd_revenue) AS exchange_rate_effect,
        po.quantity AS quantity
    FROM product_other_prices AS po
    INNER JOIN product_usd_prices AS pu
        ON pu.event_date = po.event_date
        AND pu.product_name = po.product_name
    INNER JOIN exchange_rates AS xr
        ON xr.currency_exchange_date = po.event_date
        AND xr.currency_code = po.currency_code
)

SELECT
    event_date,
    currency_code,
    product_name,
    exchange_rate, -- This should be averaged when aggregated
    exchange_rate_effect, -- This should be averaged when aggregated
    quantity AS sold_quantity
FROM currency_deviation
WHERE currency_code != 'USD'
