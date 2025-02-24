-- Query to determine if players visit the store
SELECT
    DATE_TRUNC(event_date, MONTH) AS event_month,
    COUNT(DISTINCT store_entry_device_id) AS players_visited_store,
    COUNT(DISTINCT device_id) AS total_players,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT store_entry_device_id),
            COUNT(DISTINCT device_id)
        ),
        2
    ) AS store_visit_rate
FROM `toca-data-science-assignment.ra_ae_assignment.fact_telemetry`
GROUP BY event_month
ORDER BY event_month DESC;