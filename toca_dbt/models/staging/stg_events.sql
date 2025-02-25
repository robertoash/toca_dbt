{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id',
        partition_by={
            'field': 'event_date',
            'data_type': 'date'
        },
        cluster_by=['event_name'],
        tags=['hourly']
    )
}}

WITH base AS (
    SELECT
        event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
        event_name,
        device_id,
        install_id,
        device_category,
        LOWER(TRIM(install_source)) AS install_source,
        event_params
    FROM {{ source('ae_assignment_data', 'events') }}

    {% if is_incremental() %}
        -- Pull the last 7 days of data to account for event loading delays
        WHERE
            TIMESTAMP_MICROS(event_timestamp) >= {{ incremental_window('event_timestamp', 2) }}
    {% endif %}
),

deduplicated AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['event_timestamp', 'device_id', 'event_name']) }} AS event_id,
        event_date,
        event_timestamp,
        event_name,
        -- Use MAX() to keep non-null device_id
        MAX(device_id) OVER (
            PARTITION BY event_timestamp, install_id, event_name
        ) AS device_id,
        install_id,
        device_category,
        install_source,
        -- Sort event_params to prevent duplicates
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
    FROM base
    -- Pick the row with the most keys
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY ARRAY_LENGTH(event_params) DESC
    ) = 1
)

SELECT *
FROM deduplicated