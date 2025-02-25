-- Query to determine if players visit the store

WITH store_visit_data AS (
    SELECT
        device_id,
        first_active_date,
        first_store_visit_date
    FROM `toca-data-science-assignment.ra_ae_assignment.tracker_player_behavior`
)

SELECT
    DATE_TRUNC(first_active_date, MONTH) AS first_active_month,
    COUNT(DISTINCT device_id) AS all_players,
    COUNT(DISTINCT CASE WHEN first_store_visit_date IS NOT NULL THEN device_id END) AS players_visited_store
FROM store_visit_data
GROUP BY first_active_month
ORDER BY first_active_month DESC;