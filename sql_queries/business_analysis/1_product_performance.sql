-- Query to determine which product categories are performing better and identifying star products
SELECT
    product_type,
    product_subtype,
    product_name,
    SUM(quantity) AS sales_count,
    ROUND(SUM(revenue_usd), 2) AS total_revenue_usd
FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`
GROUP BY
    product_type,
    product_subtype,
    product_name
ORDER BY total_revenue_usd DESC;