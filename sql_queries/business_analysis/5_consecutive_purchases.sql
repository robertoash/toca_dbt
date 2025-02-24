-- Query to check if players make consecutive purchases
-- Consecutive purchase is defined as a device_id that buys more than once
-- within 2 days

WITH players AS (
    SELECT
        MIN(event_date) AS cohort_date,
        device_id
    FROM `toca-data-science-assignment.ra_ae_assignment.fact_telemetry`
    GROUP BY device_id
),

rolling_purchases AS (
    SELECT
        device_id,
        event_date AS purchase_date,
        SUM(quantity) OVER (
            PARTITION BY device_id
            ORDER BY event_date
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS total_quantity_2_day
    FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`
),

consecutive_purchases AS (
    SELECT DISTINCT
        device_id,
        purchase_date
    FROM rolling_purchases
    WHERE total_quantity_2_day > 1
)

SELECT
    DATE_TRUNC(p.cohort_date, MONTH) AS cohort_month,
    COUNT(DISTINCT p.device_id) AS total_players,
    COUNT(DISTINCT rp.device_id) AS total_purchasers,
    COUNT(DISTINCT c.device_id) AS total_consecutive_purchasers,
    SAFE_DIVIDE(COUNT(DISTINCT rp.device_id), COUNT(DISTINCT p.device_id)) AS purchaser_rate,
    SAFE_DIVIDE(COUNT(DISTINCT c.device_id), COUNT(DISTINCT rp.device_id)) AS consecutive_purchaser_rate
FROM players AS p
LEFT JOIN rolling_purchases AS rp
    ON rp.device_id = p.device_id
    AND rp.purchase_date >= p.cohort_date
LEFT JOIN consecutive_purchases c
    ON rp.device_id = c.device_id
    AND rp.purchase_date = c.purchase_date
    AND c.purchase_date >= p.cohort_date
GROUP BY cohort_month
ORDER BY cohort_month DESC;
