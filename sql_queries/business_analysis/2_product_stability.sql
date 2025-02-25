-- Query to analyze if product performance is stable over time
SELECT
    product_name,
    DATE_TRUNC(purchase_date, MONTH) AS purchase_month,
    SUM(quantity) AS sales,
    SUM(revenue_usd) AS revenue_usd
FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`
GROUP BY
    product_name,
    purchase_month
ORDER BY
    product_name,
    purchase_month;