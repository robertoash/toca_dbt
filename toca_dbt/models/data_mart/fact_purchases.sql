{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='purchase_id',
        partition_by={'field': 'purchase_date', 'data_type': 'date'},
        cluster_by=['product_name', 'device_category', 'currency_code'],
        tags=['hourly']
    )
}}

WITH all_events AS (
    SELECT
        event_date,
        event_timestamp,
        device_id,
        install_id,
        device_category,
        install_source,
        event_name,
        product_name,
        quantity,
        price_local,
        currency_code,
        revenue_local
    FROM {{ ref('intm_events') }}

    {% if is_incremental() %}
      WHERE event_timestamp >= {{ incremental_window('event_timestamp', 2)}}
    {% endif %}
),

purchase_events AS (
    SELECT
        event_date AS purchase_date,
        * EXCEPT(event_date)
    FROM all_events
    WHERE event_name = 'in_app_purchase'
),

first_active_dates AS (
    SELECT
        device_id,
        MIN(event_date) AS first_active_date
    FROM all_events
    GROUP BY device_id
),

include_products AS (
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
        COALESCE(wp.price_local * er_scd.usd_per_currency, wp.price_local) AS price_usd,
        COALESCE(wp.revenue_local * er_scd.usd_per_currency, wp.revenue_local) AS revenue_usd
    FROM include_products AS wp
    LEFT JOIN {{ ref('exchange_rates_scd') }} AS er_scd
        ON wp.purchase_date BETWEEN er_scd.valid_from AND er_scd.valid_to
        AND wp.currency_code = er_scd.currency_code
)

SELECT
    {{
        dbt_utils.generate_surrogate_key(
            [
                'xr.purchase_date',
                'xr.device_id',
                'xr.product_name'
            ]
        )
    }} AS purchase_id,
    xr.purchase_date,
    xr.device_id,
    fa.first_active_date,
    xr.install_source,
    xr.device_category,
    xr.product_name,
    xr.product_type,
    xr.product_subtype,
    xr.currency_code,
    SUM(xr.quantity) AS quantity,
    ROUND(SUM(xr.revenue_local), 2) AS revenue_local,
    ROUND(SUM(xr.revenue_usd), 2) AS revenue_usd
FROM with_exchange_rates AS xr
LEFT JOIN first_active_dates AS fa
    ON xr.device_id = fa.device_id
GROUP BY
    purchase_id,
    purchase_date,
    xr.device_id,
    fa.first_active_date,
    xr.install_source,
    xr.device_category,
    xr.product_name,
    xr.product_type,
    xr.product_subtype,
    xr.currency_code
