-- Query to determine how long it takes for new players to make a purchase
WITH first_purchase_dates AS (
    SELECT
        device_id,
        MIN(event_date) AS first_purchase_date
    FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`
    GROUP BY device_id
),

first_visit_dates AS (
    SELECT
        device_id,
        MIN(event_date) AS first_visit_date
    FROM `toca-data-science-assignment.ra_ae_assignment.fact_telemetry`
    GROUP BY device_id
)

SELECT
    DATE_DIFF(f.first_purchase_date, v.first_visit_date, DAY) AS days_to_first_purchase,
    COUNT(DISTINCT f.device_id) AS player_count
FROM first_purchase_dates AS f
JOIN first_visit_dates AS v
    ON f.device_id = v.device_id
WHERE f.first_purchase_date >= v.first_visit_date
GROUP BY days_to_first_purchase
ORDER BY days_to_first_purchase;