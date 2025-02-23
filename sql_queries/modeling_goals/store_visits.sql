-- Query to determine if players visit the store
SELECT COUNT(DISTINCT install_id) AS players_visited_store
FROM `toca-data-science-assignment.ra_ae_assignment.fact_visits`
WHERE event_name = 'store_visit';