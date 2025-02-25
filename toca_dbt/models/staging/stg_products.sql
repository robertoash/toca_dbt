{{
    config(
        materialized = 'table',
        tags = ['daily']
    )
}}

WITH products AS (
    SELECT
        {{ snake_case('product_name') }} AS product_name,
        {{ snake_case('type') }} AS product_type,
        {{ snake_case('subtype') }} AS product_subtype
    FROM {{ source('ae_assignment_data', 'products') }}
)

SELECT *
FROM products

