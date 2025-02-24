-- Query to calculate revenue over time
SELECT
    DATE_TRUNC(event_date, MONTH) AS event_month,
    SUM(revenue_usd) AS total_revenue
FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`
GROUP BY event_month
ORDER BY event_month;