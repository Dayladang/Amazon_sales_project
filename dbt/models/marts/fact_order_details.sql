SELECT DISTINCT
    order_key,
    product_key,
    unit_price,
    total_amount,
    payment_method
FROM {{ ref('stg_amazon_sales') }}