-- Model Congruency Checks

-- 1. Row count comparisons between staging and source
WITH staging_counts AS (
    SELECT
        'stg_events' as model_name,
        COUNT(*) as row_count
    FROM `toca-data-science-assignment.ra_ae_assignment.stg_events`
),

-- 2. Row count comparisons between intermediate and staging
intermediate_counts AS (
    SELECT
        'intm_events' as model_name,
        COUNT(*) as row_count
    FROM `toca-data-science-assignment.ra_ae_assignment.intm_events`
),

-- 3. Row count comparisons between fact and intermediate
fact_counts AS (
    SELECT
        'fact_purchases' as model_name,
        COUNT(*) as row_count
    FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`
)

-- Combine all counts
SELECT
    model_name,
    row_count
FROM (
    SELECT * FROM staging_counts
    UNION ALL
    SELECT * FROM intermediate_counts
    UNION ALL
    SELECT * FROM fact_counts
)

-- 4. Metric sum comparisons between layers
SELECT
    'Revenue comparison' as check_name,
    SUM(revenue) as fact_revenue,
    (SELECT SUM(revenue) FROM `toca-data-science-assignment.ra_ae_assignment.intm_events`) as intm_revenue,
    (SELECT SUM(revenue) FROM `toca-data-science-assignment.ra_ae_assignment.stg_events`) as stg_revenue,
    CASE
        WHEN SUM(revenue) = (SELECT SUM(revenue) FROM `toca-data-science-assignment.ra_ae_assignment.intm_events`)
        AND (SELECT SUM(revenue) FROM `toca-data-science-assignment.ra_ae_assignment.intm_events`) =
            (SELECT SUM(revenue) FROM `toca-data-science-assignment.ra_ae_assignment.stg_events`)
        THEN 'PASS'
        ELSE 'FAIL'
    END as check_status
FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`

-- 6. Date range consistency check
SELECT
    'Date range check' as check_name,
    table_name,
    MIN(event_date) as min_date,
    MAX(event_date) as max_date
FROM (
    SELECT 'stg_events' as table_name, event_date
    FROM `toca-data-science-assignment.ra_ae_assignment.stg_events`
    UNION ALL
    SELECT 'intm_events' as table_name, event_date
    FROM `toca-data-science-assignment.ra_ae_assignment.intm_events`
    UNION ALL
    SELECT 'fact_purchases' as table_name, event_date
    FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`
)
GROUP BY table_name

-- 7. Duplicate check
SELECT
    'Duplicate check' as check_name,
    table_name,
    COUNT(*) as duplicate_count
FROM (
    -- Staging duplicates
    SELECT
        'stg_events' as table_name,
        event_id,
        COUNT(*) as count
    FROM `toca-data-science-assignment.ra_ae_assignment.stg_events`
    GROUP BY event_id
    HAVING COUNT(*) > 1
    UNION ALL
    -- Intermediate duplicates
    SELECT
        'intm_events' as table_name,
        event_id,
        COUNT(*) as count
    FROM `toca-data-science-assignment.ra_ae_assignment.intm_events`
    GROUP BY event_id
    HAVING COUNT(*) > 1
    UNION ALL
    -- Fact duplicates
    SELECT
        'fact_purchases' as table_name,
        purchase_id,
        COUNT(*) as count
    FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases`
    GROUP BY purchase_id
    HAVING COUNT(*) > 1
)
GROUP BY table_name

-- 8. Referential integrity check
SELECT
    'Referential integrity check' as check_name,
    'fact_purchases -> intm_events' as relationship,
    COUNT(*) as orphaned_records
FROM `toca-data-science-assignment.ra_ae_assignment.fact_purchases` f
LEFT JOIN `toca-data-science-assignment.ra_ae_assignment.intm_events` i
    ON f.event_id = i.event_id
WHERE i.event_id IS NULL

UNION ALL

SELECT
    'Referential integrity check' as check_name,
    'intm_events -> stg_events' as relationship,
    COUNT(*) as orphaned_records
FROM `toca-data-science-assignment.ra_ae_assignment.intm_events` i
LEFT JOIN `toca-data-science-assignment.ra_ae_assignment.stg_events` s
    ON i.event_id = s.event_id
WHERE s.event_id IS NULL

-- 9. Source to Staging comparison
SELECT
    'Source to Staging comparison' as check_name,
    'Row count check' as check_type,
    source.source_count,
    staging.staging_count,
    CASE
        WHEN source.source_count = staging.staging_count THEN 'PASS'
        ELSE 'FAIL'
    END as check_status
FROM
    (SELECT COUNT(*) as source_count
     FROM `toca-data-science-assignment.ae_assignment_data.events`) source,
    (SELECT COUNT(*) as staging_count
     FROM `toca-data-science-assignment.ra_ae_assignment.stg_events`) staging;
