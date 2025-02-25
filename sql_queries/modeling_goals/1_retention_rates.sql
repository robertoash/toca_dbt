-- Query to calculate retention rates for D1, D3, and D7 using the retention_over_time model
SELECT *
FROM (
    SELECT
        DATE_TRUNC(first_purchase_date, MONTH) AS first_purchase_month,
        funnel_step,
        COUNT(DISTINCT device_id) AS player_count
    FROM `toca-data-science-assignment.ra_ae_assignment.tracker_player_behavior`
    WHERE
        funnel_step IN ('4 Made Consecutive Purchase', '2 Converted to Payers')
    GROUP BY
        first_purchase_month,
        funnel_step
)
PIVOT (
    SUM(player_count)
    FOR funnel_step IN (
        '2 Converted to Payers',
        '4 Made Consecutive Purchase'
    )
)
ORDER BY first_purchase_month DESC;