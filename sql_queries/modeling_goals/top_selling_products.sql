-- Query to get top-selling products
SELECT
    product_name,
    COUNT(*) AS sales_count,
    SUM(price_usd) AS total_revenue
FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`
GROUP BY product_name
ORDER BY total_revenue DESC;