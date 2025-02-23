{{
    config(
        materialized = 'table',
        primary_key = 'event_id',
        partition_by = {"field": "event_date", "data_type": "DATE"},
        cluster_by = 'event_name',
        tags = ['hourly']
    )
}}

WITH base AS (
    SELECT
        {{ primary_key_gen(['event_timestamp', 'install_id', 'event_name']) }} AS event_id,
        event_date,
        event_timestamp,
        event_name,
        device_id,
        install_id,
        device_category,
        LOWER(TRIM(install_source)) AS install_source,
        event_params
    FROM {{ source('ae_assignment_data', 'events') }}
),

deduplicated AS (
    SELECT
        event_date,
        event_timestamp,
        event_name,
        -- Use MAX() to ensure the non-null device_id is retained
        MAX(device_id) OVER (
            PARTITION BY event_id
        ) AS device_id,
        install_id,
        device_category,
        install_source,
        -- Sort event_params while keeping the original structure
        (
            SELECT ARRAY_AGG(
                STRUCT(
                    key,
                    STRUCT(
                        value.string_value,
                        value.int_value,
                        value.float_value,
                        value.double_value
                    ) AS value
                )
                ORDER BY key
            )
            FROM UNNEST(event_params)
        ) AS event_params,
        -- Assign row numbers to pick the row with the most keys
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY ARRAY_LENGTH(event_params) DESC
        ) AS row_num
    FROM base
    -- Deduplicate by picking the row with the most keys
    QUALIFY row_num = 1
)

SELECT
    *
FROM deduplicated