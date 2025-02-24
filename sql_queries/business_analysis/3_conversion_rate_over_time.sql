-- Query to analyze the proportion of players converting to paying players over time
WITH all_visits AS (
    SELECT
        v.device_id,
        MIN(v.event_date) AS first_visit_date
    FROM `toca-data-science-assignment.ra_ae_assignment.fact_telemetry` AS v
    GROUP BY v.device_id
),

first_purchase_dates AS (
    SELECT
        fp.device_id,
        MIN(fp.event_date) AS first_purchase_after_visit_date
    FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases` AS fp
    INNER JOIN all_visits AS av
        ON fp.device_id = av.device_id
    WHERE av.first_visit_date <= fp.event_date
    GROUP BY device_id
),

purchase_after_visit AS (
    SELECT
        DATE_TRUNC(av.first_visit_date, MONTH) AS first_visit_month,
        COUNT(DISTINCT av.device_id) AS total_players_in_cohort,
        COUNT(DISTINCT fp.device_id) AS converted_players,
        COUNT(DISTINCT
            CASE WHEN DATE_DIFF(fp.first_purchase_after_visit_date, av.first_visit_date, WEEK) = 0
            THEN av.device_id
            ELSE NULL
            END
        ) AS w0_converted_players,
        COUNT(DISTINCT
            CASE WHEN DATE_DIFF(fp.first_purchase_after_visit_date, av.first_visit_date, WEEK) = 1
            THEN av.device_id
            ELSE NULL
            END
        ) AS w1_converted_players,
        COUNT(DISTINCT
            CASE WHEN DATE_DIFF(fp.first_purchase_after_visit_date, av.first_visit_date, WEEK) = 2
            THEN av.device_id
            ELSE NULL
            END
        ) AS w2_converted_players,
        COUNT(DISTINCT
            CASE WHEN DATE_DIFF(fp.first_purchase_after_visit_date, av.first_visit_date, WEEK) = 3
            THEN av.device_id
            ELSE NULL
            END
        ) AS w3_converted_players,
        COUNT(DISTINCT
            CASE WHEN DATE_DIFF(fp.first_purchase_after_visit_date, av.first_visit_date, WEEK) = 4
            THEN av.device_id
            ELSE NULL
            END
        ) AS w4_converted_players
    FROM all_visits AS av
    LEFT JOIN first_purchase_dates AS fp
        ON fp.device_id = av.device_id
    GROUP BY
        first_visit_month
)

SELECT
    first_visit_month,
    total_players_in_cohort,
    converted_players,
    total_players_in_cohort - converted_players AS non_converted_players,
    w0_converted_players,
    w1_converted_players,
    w2_converted_players,
    w3_converted_players,
    w4_converted_players,
    ROUND(
        SAFE_DIVIDE(converted_players, total_players_in_cohort),
        2
    ) AS conversion_rate,
    ROUND(
        SAFE_DIVIDE(total_players_in_cohort - converted_players, total_players_in_cohort),
        2
    ) AS non_conversion_rate,
    ROUND(SAFE_DIVIDE(w0_converted_players, total_players_in_cohort), 2) AS w0_conversion_rate,
    ROUND(SAFE_DIVIDE(w1_converted_players, total_players_in_cohort), 2) AS w1_conversion_rate,
    ROUND(SAFE_DIVIDE(w2_converted_players, total_players_in_cohort), 2) AS w2_conversion_rate,
    ROUND(SAFE_DIVIDE(w3_converted_players, total_players_in_cohort), 2) AS w3_conversion_rate,
    ROUND(SAFE_DIVIDE(w4_converted_players, total_players_in_cohort), 2) AS w4_conversion_rate
FROM purchase_after_visit
ORDER BY first_visit_month DESC;