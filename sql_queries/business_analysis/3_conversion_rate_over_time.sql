-- Query to calculate conversion rate over time
SELECT *
FROM (
    SELECT
        DATE_TRUNC(first_active_date, MONTH) AS first_active_month,
        funnel_step,
        COUNT(DISTINCT device_id) AS player_count
    FROM `ra_ae_assignment.tracker_player_behavior`
    GROUP BY
        first_active_month,
        funnel_step
)
PIVOT (
    SUM(player_count)
    FOR funnel_step IN (
        '1 Total Players',
        '2 Converted to Payers',
        '3 Visited Store After Purchase',
        '4 Made Consecutive Purchase'
        'Non-Converted Players'
    )
)
ORDER BY first_active_month;