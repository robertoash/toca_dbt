-- Query to calculate revenue over time
SELECT
    DATE_TRUNC(purchase_date, MONTH) AS purchase_month,
    SUM(revenue_usd) AS total_revenue
FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`
GROUP BY purchase_month
ORDER BY purchase_month;