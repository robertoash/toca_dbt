-- Query to check if players visit the store after making a purchase
WITH purchase_dates AS (
    SELECT install_id, MIN(event_date) AS first_purchase_date
    FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`
    GROUP BY install_id
),

post_purchase_visits AS (
    SELECT v.install_id, v.event_date
    FROM `toca-data-science-assignment.ra_ae_assignment.fact_visits` AS v
    JOIN purchase_dates p ON v.install_id = p.install_id
    WHERE v.event_date > p.first_purchase_date
    AND v.event_name = 'store_visit'
)

SELECT COUNT(DISTINCT install_id) AS players_visited_store_after_purchase
FROM post_purchase_visits;