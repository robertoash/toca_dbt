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
        device_category,
        install_source,
        event_name,
        product_name,
        quantity,
        price_local,
        currency_code,
        revenue_local
    FROM {{ ref('intm_events') }}
    WHERE event_name = 'in_app_purchase'
),

with_products AS (
    SELECT
        pe.*,
        p.product_type,
        p.product_subtype
    FROM purchase_events AS pe
    LEFT JOIN {{ ref('stg_products') }} AS p
        ON pe.product_name = p.product_name
),

with_exchange_rates AS (
    SELECT
        wp.*,
        COALESCE(wp.price_local * er.usd_per_currency, wp.price_local) AS price_usd,
        COALESCE(wp.revenue_local * er.usd_per_currency, wp.revenue_local) AS revenue_usd
    FROM with_products AS wp
    LEFT JOIN {{ ref('stg_exchange_rates') }} AS er
        ON wp.event_date = er.currency_exchange_date
        AND wp.currency_code = er.currency_code
)

SELECT
    event_date,
    install_source,
    device_category,
    product_name,
    product_type,
    product_subtype,
    quantity,
    price_local,
    currency_code,
    revenue_local,
    price_usd,
    revenue_usd
FROM with_exchange_rates
