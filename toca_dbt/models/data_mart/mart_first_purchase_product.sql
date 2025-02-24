
{{
    config(
        materialized = 'view'
    )
}}

WITH first_purchases_by_product AS (
    SELECT
        device_id,
        product_name,
        MIN(event_timestamp) AS first_purchase_timestamp
    FROM {{ ref('intm_purchase_events') }}
    GROUP BY
        device_id,
        product_name
),

first_overall_purchase AS (
    SELECT
        device_id,
        product_name,
        first_purchase_timestamp,
    FROM first_purchases_by_product
    QUALIFY RANK() OVER (PARTITION BY device_id ORDER BY first_purchase_timestamp) = 1
),

first_purchase_counts AS (
    SELECT
        DATE(TIMESTAMP_MICROS(first_purchase_timestamp)) AS purchase_date,
        product_name,
        COUNT(DISTINCT device_id) AS product_first_time_purchases
    FROM first_overall_purchase
    GROUP BY
        purchase_date,
        product_name
)

SELECT
    fpc.purchase_date AS first_purchase_date,
    fpc.product_name,
    fpc.product_first_time_purchases AS product_first_time_purchases
FROM first_purchase_counts AS fpc
ORDER BY product_first_time_purchases DESC
