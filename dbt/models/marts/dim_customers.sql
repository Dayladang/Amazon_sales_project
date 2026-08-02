SELECT DISTINCT
    customer_key,
    customer_id,
    customer_name,
    city,
    state,
    country
FROM {{ ref('stg_amazon_sales') }}