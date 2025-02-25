-- Query to check if players visit the store after making a purchase
SELECT *
FROM (
    SELECT
        DATE_TRUNC(first_purchase_date, MONTH) AS first_purchase_month,
        funnel_step,
        COUNT(DISTINCT device_id) AS player_count
    FROM `toca-data-science-assignment.ra_ae_assignment.tracker_player_behavior`
    WHERE
        funnel_step IN (
        '2 Converted to Payers',
        '3 Visited Store After Purchase'
    )
    GROUP BY
        first_purchase_month,
        funnel_step
)
PIVOT (
    SUM(player_count)
    FOR funnel_step IN (
        '2 Converted to Payers',
        '3 Visited Store After Purchase'
    )
)
ORDER BY first_purchase_month DESC;