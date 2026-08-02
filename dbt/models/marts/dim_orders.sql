SELECT DISTINCT
    order_key,
    order_date,
    shipping_cost,
    order_status,
    customer_key,
    seller_key
FROM {{ ref('stg_amazon_sales') }}