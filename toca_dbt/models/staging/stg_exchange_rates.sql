{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='exchange_rate_id',
        partition_by={
            'field': 'currency_exchange_date',
            'data_type': 'date'
        },
        cluster_by=['currency_code'],
        tags=['daily']
    )
}}

WITH exchange_rates AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['dt', 'currency_code']) }} AS exchange_rate_id,
        dt AS currency_exchange_date,
        currency_code,
        usd_per_currency,
        is_extrapolated
    FROM {{ source('ae_assignment_data', 'exchange_rates') }}
    {% if is_incremental() %}
        -- Load last 2 days in case of late loading or fx market timezone differences
        WHERE dt >= GREATEST(
            DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY),
            (SELECT IFNULL(MAX(currency_exchange_date), DATE('1970-01-01')) FROM {{ this }})
        )
    {% endif %}
)

SELECT * FROM exchange_rates
