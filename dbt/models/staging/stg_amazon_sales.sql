{{
    config(    
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',

        partition_by={
            "field": "order_date",
            "data_type": "date",
            "granularity": "day"
        },

        cluster_by=['order_id', 'customer_id']
    )
}}

with source_data as (
    SELECT 
        -- Create surrogate keys for the dimension tables
        {{ dbt_utils.generate_surrogate_key(['OrderID']) }} as order_key,
        {{ dbt_utils.generate_surrogate_key(['CustomerID']) }} as customer_key,
        {{ dbt_utils.generate_surrogate_key(['ProductID']) }} as product_key,
        {{ dbt_utils.generate_surrogate_key(['SellerID']) }} as seller_key,

        -- add a timestamp for when the record was inserted into the staging table
        current_timestamp() as inserted_at,

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

    -- CDC logic for incremental updates
    {% if is_incremental() %}
        AND OrderDate > (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}
)

-- using QUALIFY from BigQuery to filter out duplicates based on 
-- the latest inserted_at timestamp for each order_id BEFORE MERGING into the target table.
SELECT *
FROM source_data
qualify row_number() over (
    partition by order_id
    order by order_date desc, inserted_at desc
) = 1