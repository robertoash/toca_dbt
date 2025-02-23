
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
    GROUP BY 1, 2
),

ranked_purchases AS (
    SELECT
        device_id,
        product_name,
        first_purchase_time,
        RANK() OVER (PARTITION BY device_id ORDER BY first_purchase_time) AS purchase_rank
    FROM first_purchases
)

SELECT
    product_name,
    COUNT(*) AS first_time_purchases
FROM ranked_purchases
WHERE purchase_rank = 1
GROUP BY 1
ORDER BY first_time_purchases DESC;