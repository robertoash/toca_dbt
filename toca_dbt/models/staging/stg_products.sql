{{
    config(
        materialized = 'view'
    )
}}

WITH products AS (
    SELECT
        LOWER(TRIM(product_name)) AS product_name,
        LOWER(TRIM(type)) AS product_type,
        LOWER(TRIM(subtype)) AS product_subtype
    FROM {{ source('ae_assignment_data', 'products') }}
)

SELECT * FROM products

