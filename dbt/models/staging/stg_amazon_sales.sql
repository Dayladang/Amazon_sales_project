SELECT 
    -- Create surrogate keys for the dimension tables
    {{ dbt_utils.generate_surrogate_key(['OrderID']) }} as order_key,
    {{ dbt_utils.generate_surrogate_key(['CustomerID']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['ProductID']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['SellerID']) }} as seller_key,

    -- original columns for the source table
    OrderID as order_id,
    OrderDate as order_date,
    CustomerID as customer_id,
    CustomerName as customer_name,
    ProductID as product_id,
    ProductName as product_name,
    Category as category,
    Brand as brand,
    Quantity as quantity,
    UnitPrice as unit_price,
    Discount as discount,
    Tax as tax,
    ShippingCost as shipping_cost,
    TotalAmount as total_amount,
    PaymentMethod as payment_method,
    OrderStatus as order_status,
    City as city,
    State as state,
    Country as country,
    SellerID as seller_id
FROM {{ source('bigquery_raw_sources', 'silver_amazon_sales') }}
WHERE OrderID IS NOT NULL
