-- Query to calculate revenue over time
SELECT event_date, SUM(price_usd) AS total_revenue
FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`
GROUP BY event_date
ORDER BY event_date;