{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'product_exchange_rate_id',
        partition_by = {"field": "purchase_date", "data_type": "DATE"},
        cluster_by = ['currency_code', 'product_name'],
        tags = ['daily']
    )
}}

WITH purchases AS (
    SELECT
        purchase_date,
        product_name,
        currency_code,
        revenue_usd,
        SUM(quantity) AS quantity
    FROM {{ ref('fact_purchases') }}
    {% if is_incremental() %}
        WHERE TIMESTAMP(purchase_date) >= {{
            incremental_window('TIMESTAMP(purchase_date)', 2)
        }}
    {% endif %}
    GROUP BY
        purchase_date,
        product_name,
        currency_code,
        revenue_usd
),

product_usd_prices AS (
    -- Get the average revenue in USD for each product when purchased in USD
    SELECT
        purchase_date,
        product_name,
        AVG(revenue_usd) AS avg_usd_revenue
    FROM purchases
    WHERE currency_code = 'USD'
    GROUP BY
        purchase_date,
        product_name
),

product_other_prices AS (
    -- Get the average revenue in USD for each product when purchased in USD
    SELECT
        purchase_date,
        product_name,
        currency_code,
        AVG(revenue_usd) AS avg_usd_revenue,
        SUM(quantity) AS quantity
    FROM purchases
    WHERE currency_code != 'USD'
    GROUP BY
        purchase_date,
        product_name,
        currency_code
),

currency_deviation AS (
    -- Compare the average revenue in USD for the same products sold in other currencies
    SELECT
        pu.purchase_date,
        pu.product_name,
        po.currency_code,
        xr.exchange_rate,
        pu.avg_usd_revenue AS avg_revenue_usd,
        po.avg_usd_revenue AS avg_revenue_usd_incl_xr,
        SAFE_DIVIDE(po.avg_usd_revenue, pu.avg_usd_revenue) AS exchange_rate_effect,
        po.quantity AS quantity
    FROM product_other_prices AS po
    INNER JOIN product_usd_prices AS pu
        ON pu.purchase_date = po.purchase_date
        AND pu.product_name = po.product_name
    INNER JOIN {{ ref('exchange_rates_scd') }} AS xr
        ON po.purchase_date BETWEEN xr.valid_from AND xr.valid_to
        AND xr.currency_code = po.currency_code
)

SELECT
    {{
        dbt_utils.generate_surrogate_key(
            ['purchase_date',
            'currency_code',
            'product_name']
        )
    }} AS product_exchange_rate_id,
    purchase_date,
    currency_code,
    product_name,
    exchange_rate, -- This should be averaged when aggregated
    exchange_rate_effect, -- This should be averaged when aggregated
    quantity AS sold_quantity
FROM currency_deviation
WHERE currency_code != 'USD'
