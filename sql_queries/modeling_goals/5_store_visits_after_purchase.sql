-- Query to check if players visit the store after making a purchase
WITH first_purchase_dates AS (
    SELECT
        device_id,
        MIN(event_date) AS first_purchase_date
    FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`
    GROUP BY device_id
),

post_purchase_visits AS (
    SELECT
        v.device_id,
        MIN(v.event_date) AS first_visit_after_purchase_date
    FROM `toca-data-science-assignment.ra_ae_assignment.fact_telemetry` AS v
    INNER JOIN first_purchase_dates AS p
        ON v.device_id = p.device_id
    WHERE
        v.event_date > p.first_purchase_date
        AND v.store_entry_device_id IS NOT NULL
    GROUP BY v.device_id
),

recurring_after_purchase AS (
    SELECT
        DATE_TRUNC(fp.first_purchase_date, MONTH) AS first_purchase_month,
        COUNT(DISTINCT fp.device_id) AS total_players_in_cohort,
        COUNT(DISTINCT v.device_id) AS returned_after_purchase,
        COUNT(DISTINCT
            CASE WHEN DATE_DIFF(v.first_visit_after_purchase_date, fp.first_purchase_date, WEEK) = 0
            THEN v.device_id
            ELSE NULL
            END
        ) AS w0_recurring_players,
        COUNT(DISTINCT
            CASE WHEN DATE_DIFF(v.first_visit_after_purchase_date, fp.first_purchase_date, WEEK) = 1
            THEN v.device_id
            ELSE NULL
            END
        ) AS w1_recurring_players,
        COUNT(DISTINCT
            CASE WHEN DATE_DIFF(v.first_visit_after_purchase_date, fp.first_purchase_date, WEEK) = 2
            THEN v.device_id
            ELSE NULL
            END
        ) AS w2_recurring_players,
        COUNT(DISTINCT
            CASE WHEN DATE_DIFF(v.first_visit_after_purchase_date, fp.first_purchase_date, WEEK) = 3
            THEN v.device_id
            ELSE NULL
            END
        ) AS w3_recurring_players,
        COUNT(DISTINCT
            CASE WHEN DATE_DIFF(v.first_visit_after_purchase_date, fp.first_purchase_date, WEEK) = 4
            THEN v.device_id
            ELSE NULL
            END
        ) AS w4_recurring_players
    FROM first_purchase_dates AS fp
    LEFT JOIN post_purchase_visits AS v
        ON fp.device_id = v.device_id
    GROUP BY
        first_purchase_month
)

SELECT
    first_purchase_month,
    total_players_in_cohort,
    returned_after_purchase,
    total_players_in_cohort - returned_after_purchase AS non_returned_players,
    w0_recurring_players,
    w1_recurring_players,
    w2_recurring_players,
    w3_recurring_players,
    w4_recurring_players,
    ROUND(
        SAFE_DIVIDE(returned_after_purchase, total_players_in_cohort),
        2
    ) AS recurring_rate,
    ROUND(
        SAFE_DIVIDE(total_players_in_cohort - returned_after_purchase, total_players_in_cohort),
        2
    ) AS non_recurring_rate,
    ROUND(SAFE_DIVIDE(w0_recurring_players, total_players_in_cohort), 2) AS w0_recurring_rate,
    ROUND(SAFE_DIVIDE(w1_recurring_players, total_players_in_cohort), 2) AS w1_recurring_rate,
    ROUND(SAFE_DIVIDE(w2_recurring_players, total_players_in_cohort), 2) AS w2_recurring_rate,
    ROUND(SAFE_DIVIDE(w3_recurring_players, total_players_in_cohort), 2) AS w3_recurring_rate,
    ROUND(SAFE_DIVIDE(w4_recurring_players, total_players_in_cohort), 2) AS w4_recurring_rate
FROM recurring_after_purchase
ORDER BY first_purchase_month DESC;