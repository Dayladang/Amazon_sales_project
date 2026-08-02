SELECT DISTINCT
    product_key,
    product_name,
    category,
    brand,
    quantity,
    discount,
    tax
FROM {{ ref('stg_amazon_sales') }}