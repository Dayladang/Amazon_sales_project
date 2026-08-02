SELECT DISTINCT
    seller_key,
    seller_id
FROM {{ ref('stg_amazon_sales') }}