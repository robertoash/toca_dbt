-- Query to analyze if product performance is stable over time
SELECT
    p.product_name,
    DATE_TRUNC(f.event_date, MONTH) AS event_month,
    SUM(f.quantity) AS sales,
    SUM(f.revenue_usd) AS revenue_usd
FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases` AS f
JOIN `toca-data-science-assignment.ra_ae_assignment.dim_product` AS p
    ON f.product_name = p.product_name
GROUP BY
    p.product_name,
    event_month
ORDER BY
    p.product_name,
    event_month;