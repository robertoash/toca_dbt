{{
    config(
        materialized = 'table',
        partition_by = {"field": "event_date", "data_type": "DATE"},
        cluster_by = ['product_name', 'device_category', 'install_source'],
        tags = ['hourly']
    )
}}

WITH purchase_events AS (
    SELECT
        event_date,
        device_id,
        device_category,
        install_source,
        event_name,
        product_name,
        quantity,
        price_local,
        currency_code,
        revenue_local
    FROM {{ ref('intm_purchase_events') }}
),

with_products AS (
    SELECT
        pe.*,
        p.product_type,
        p.product_subtype
    FROM purchase_events AS pe
    LEFT JOIN {{ ref('dim_product') }} AS p
        ON pe.product_name = p.product_name
),

with_exchange_rates AS (
    SELECT
        wp.*,
        COALESCE(wp.price_local * er.usd_per_currency, wp.price_local) AS price_usd,
        COALESCE(wp.revenue_local * er.usd_per_currency, wp.revenue_local) AS revenue_usd
    FROM with_products AS wp
    LEFT JOIN {{ ref('dim_exchange_rates') }} AS er
        ON wp.event_date = er.currency_exchange_date
        AND wp.currency_code = er.currency_code
)

SELECT
    event_date,
    device_id,
    install_source,
    device_category,
    product_name,
    product_type,
    product_subtype,
    currency_code,
    SUM(quantity) AS quantity,
    ROUND(SUM(revenue_local), 2) AS revenue_local,
    ROUND(SUM(revenue_usd), 2) AS revenue_usd
FROM with_exchange_rates
GROUP BY
    event_date,
    device_id,
    install_source,
    device_category,
    product_name,
    product_type,
    product_subtype,
    currency_code