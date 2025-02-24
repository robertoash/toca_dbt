-- Query to determine which products convert players to paying customers
WITH first_purchase_counts AS (
    SELECT
        DATE_TRUNC(first_purchase_date, MONTH) AS first_purchase_month,
        product_name,
        SUM(product_first_time_purchases) AS product_first_time_purchases,
    FROM `toca-data-science-assignment.ra_ae_assignment.mart_first_purchase_product`
    GROUP BY
        first_purchase_month,
        product_name
)

SELECT
    first_purchase_month,
    product_name,
    product_first_time_purchases,
    SUM(product_first_time_purchases) OVER(
            PARTITION BY first_purchase_month
    ) AS total_first_time_purchases,
    ROUND(
        SAFE_DIVIDE(
            product_first_time_purchases,
            SUM(product_first_time_purchases) OVER(
                PARTITION BY first_purchase_month
            )
        ), 2
    ) AS product_first_time_purchase_contribution
FROM first_purchase_counts
ORDER BY
    first_purchase_month DESC,
    product_first_time_purchase_contribution DESC;