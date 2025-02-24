-- Query to calculate retention rates for D1, D3, and D7 using the retention_over_time model
SELECT
    DATE_TRUNC(install_date, MONTH) AS install_month,
    SUM(d0_users) AS d0_users,
    SUM(d1_retained_users) AS d1_retained_users,
    SUM(d3_retained_users) AS d3_retained_users,
    SUM(d7_retained_users) AS d7_retained_users,
    ROUND(SAFE_DIVIDE(SUM(d1_retained_users), SUM(d0_users)), 2) AS d1_retention_rate,
    ROUND(SAFE_DIVIDE(SUM(d3_retained_users), SUM(d0_users)), 2) AS d3_retention_rate,
    ROUND(SAFE_DIVIDE(SUM(d7_retained_users), SUM(d0_users)), 2) AS d7_retention_rate
FROM `toca-data-science-assignment.ra_ae_assignment.mart_retention_over_time`
GROUP BY install_month
ORDER BY install_month DESC;