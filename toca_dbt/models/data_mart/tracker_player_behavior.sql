{{
    config(
        materialized = 'table',
        partition_by = {"field": "first_active_date", "data_type": "DATE"},
        cluster_by = ['first_purchase_date'],
        tags = ['daily']
    )
}}

WITH event_data AS (
    SELECT
        device_id,
        event_date,
        event_name
    FROM {{ ref('intm_all_events') }}
),

purchase_data AS (
    SELECT
        device_id,
        purchase_date,
        product_name,
        product_type,
        product_subtype,
        LEAD(purchase_date) OVER (
            PARTITION BY device_id
            ORDER BY purchase_date
        ) AS next_purchase_date
    FROM {{ ref('fact_purchases') }}
),

store_visitors AS (
    SELECT
        device_id,
        event_date,
        MIN(event_date) OVER (PARTITION BY device_id) AS first_store_visit_date
    FROM event_data
    WHERE event_name = 'store_entry'
    GROUP BY
        device_id,
        event_date
),

all_players AS (
    SELECT
        device_id,
        MIN(event_date) AS first_active_date
    FROM event_data
    GROUP BY device_id
),

first_conversion AS (
    SELECT
        device_id,
        purchase_date AS first_purchase_date,
        product_name AS first_purchase_product,
        product_type AS first_purchase_product_type,
        product_subtype AS first_purchase_product_subtype
    FROM purchase_data
    QUALIFY RANK() OVER (
        PARTITION BY device_id
        ORDER BY purchase_date
    ) = 1
),

store_after_purchase AS (
    SELECT
        p.device_id
    FROM purchase_data AS p
    INNER JOIN store_visitors t
        ON p.device_id = t.device_id
        AND t.event_date > p.purchase_date
),

consecutive_purchases AS (
    SELECT
        p.device_id
    FROM purchase_data AS p
    WHERE
        p.next_purchase_date IS NOT NULL
        AND DATE_DIFF(p.next_purchase_date, p.purchase_date, DAY) <= 2
),

funnel_steps AS (
    -- Step 1: All Players
    SELECT
        ap.device_id,
        '1 Total Players' AS funnel_step
    FROM all_players ap

    UNION ALL

    -- Step 2: Converted Players
    SELECT
        fc.device_id,
        '2 Converted to Payers' AS funnel_step
    FROM first_conversion fc

    UNION ALL

    -- Step 3: Store After Purchase
    SELECT
        sap.device_id,
        '3 Visited Store After Purchase' AS funnel_step
    FROM store_after_purchase sap

    UNION ALL

    -- Step 4: Consecutive Purchases
    SELECT
        cp.device_id,
        '4 Made Consecutive Purchase' AS funnel_step
    FROM consecutive_purchases cp

    UNION ALL

    -- Non-Converted Players
    SELECT
        ap.device_id,
        'Non-Converted Players' AS funnel_step
    FROM all_players ap
    LEFT JOIN first_conversion fc ON ap.device_id = fc.device_id
    WHERE fc.device_id IS NULL
),

final_data AS (
    SELECT
        fs.device_id, -- This is meant to be count distincted when aggregated
        ap.first_active_date,
        sv.first_store_visit_date,
        fc.first_purchase_date,
        fc.first_purchase_product,
        fc.first_purchase_product_type,
        fc.first_purchase_product_subtype,
        fs.funnel_step
    FROM funnel_steps fs
    INNER JOIN all_players ap ON fs.device_id = ap.device_id
    LEFT JOIN first_conversion fc ON fs.device_id = fc.device_id
    LEFT JOIN store_visitors sv ON fs.device_id = sv.device_id
)

SELECT *
FROM final_data
