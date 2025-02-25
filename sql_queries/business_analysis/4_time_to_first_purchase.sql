-- Query to determine how long it takes for new players to make a purchase
WITH first_purchase_dates AS (
    SELECT
        device_id,
        first_purchase_date,
        DATE_TRUNC(first_active_date, MONTH) AS first_active_month,
        DATE_DIFF(first_purchase_date, first_active_date, DAY) AS days_to_first_purchase,
    FROM `toca-data-science-assignment.ra_ae_assignment.tracker_player_behavior`
)

SELECT
    first_active_month,
    AVG(days_to_first_purchase) AS avg_days_to_first_purchase
FROM first_purchase_dates
WHERE first_purchase_date IS NOT NULL
GROUP BY first_active_month
ORDER BY first_active_month DESC;