-- Query to get top-selling products
SELECT
    product_name,
    COUNT(*) AS sales_count,
    SUM(revenue_usd) AS total_revenue_usd
FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`
GROUP BY product_name
ORDER BY total_revenue_usd DESC;