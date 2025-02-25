{{
    config(
        materialized = 'table',
        tags = ['daily']
    )
}}

SELECT
    product_name,
    product_type,
    product_subtype
FROM {{ ref('stg_products') }}
