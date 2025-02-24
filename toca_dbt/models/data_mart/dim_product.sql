{{
    config(
        materialized = 'view',
    )
}}

SELECT
    product_name,
    product_type,
    product_subtype
FROM {{ ref('stg_products') }}
