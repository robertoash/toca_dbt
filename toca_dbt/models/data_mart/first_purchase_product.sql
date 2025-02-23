
{{
    config(
        materialized = 'view'
    )
}}

WITH first_purchases AS (
    SELECT
        device_id,
        product_name,
        MIN(event_timestamp) AS first_purchase_time
    FROM {{ ref('intm_events') }}
    WHERE event_name = 'in_app_purchase'
    GROUP BY
        device_id,
        product_name
),

ranked_purchases AS (
    SELECT
        device_id,
        product_name,
        first_purchase_time,
        RANK() OVER (PARTITION BY device_id ORDER BY first_purchase_time) AS purchase_rank
    FROM first_purchases
),

first_purchase_counts AS (
    SELECT
        DATE(TIMESTAMP_MICROS(first_purchase_time)) AS purchase_date,
        product_name,
        COUNT(DISTINCT device_id) AS first_time_purchases
    FROM ranked_purchases
    WHERE purchase_rank = 1
    GROUP BY
        purchase_date,
        product_name
),

total_first_purchases AS (
    SELECT
        purchase_date,
        SUM(first_time_purchases) AS total_purchases
    FROM first_purchase_counts
    GROUP BY purchase_date
),

aggregate_first_purchase_counts AS (
    SELECT
        fpc.purchase_date AS first_purchase_date,
        fpc.product_name,
        tfp.total_purchases AS total_first_time_purchases,
        fpc.first_time_purchases AS product_first_time_purchases
FROM first_purchase_counts AS fpc
LEFT JOIN total_first_purchases AS tfp
    ON fpc.purchase_date = tfp.purchase_date
)

SELECT *
FROM aggregate_first_purchase_counts
ORDER BY product_first_time_purchases DESC
