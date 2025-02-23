-- Query to determine which products convert players to paying customers using first_purchase_product model
SELECT
    DATE_TRUNC(first_purchase_date, MONTH) AS first_purchase_month,
    product_name,
    SUM(total_first_time_purchases) AS total_first_time_purchases,
    SUM(product_first_time_purchases) AS product_first_time_purchases,
    ROUND(
        SAFE_DIVIDE(
            SUM(product_first_time_purchases),
            SUM(total_first_time_purchases)
        )
        , 2
    ) AS product_first_time_purchase_contribution
FROM `toca-data-science-assignment.ra_ae_assignment.first_purchase_product`
GROUP BY
    first_purchase_month,
    product_name
ORDER BY
    first_purchase_month DESC,
    product_first_time_purchase_contribution DESC;