-- Query to determine which product categories are performing better and identifying star products
SELECT
    p.product_type,
    p.product_subtype,
    p.product_name,
    SUM(f.quantity) AS sales_count,
    ROUND(SUM(f.revenue_usd), 2) AS total_revenue_usd
FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases` AS f
JOIN `toca-data-science-assignment.ra_ae_assignment.dim_product` AS p
    ON f.product_name = p.product_name
GROUP BY
    p.product_type,
    p.product_subtype,
    p.product_name
ORDER BY total_revenue_usd DESC;