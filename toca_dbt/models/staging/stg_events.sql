{{
    config(
        materialized = 'view'
    )
}}

WITH events AS (
    SELECT
        event_date,
        event_timestamp,
        event_name,
        event_params,
        device_id,
        install_id,
        device_category,
        install_source
    FROM {{ source('ae_assignment_data', 'events') }}
)

SELECT * FROM events
