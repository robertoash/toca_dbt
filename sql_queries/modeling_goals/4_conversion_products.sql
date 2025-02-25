-- Query to determine which products convert players to paying customers
WITH first_purchase_data AS (
    SELECT
        device_id,
        DATE_TRUNC(first_purchase_date, MONTH) AS first_purchase_month,
        first_purchase_product,
    FROM `toca-data-science-assignment.ra_ae_assignment.tracker_player_behavior`
    WHERE funnel_step = '2 Converted to Payers'
    AND first_purchase_product IS NOT NULL
)

SELECT
    first_purchase_month,
    first_purchase_product,
    COUNT(DISTINCT device_id) AS first_purchasers
FROM first_purchase_data
GROUP BY
    first_purchase_month,
    first_purchase_product
ORDER BY
    first_purchase_month DESC,
    first_purchasers DESC;