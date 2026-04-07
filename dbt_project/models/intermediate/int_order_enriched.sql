-- int_order_enriched: Join orders with customer and product context
-- Builds the enriched order fact used for mart-layer aggregations
-- Materialization: table

{{ config(materialized='table', tags=['intermediate']) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

enriched as (
    select
        -- Order keys
        o.order_id,
        o.customer_id,
        o.product_id,
        o.sales_rep_id,

        -- Order attributes
        o.order_date,
        o.ship_date,
        o.order_status,
        o.total_amount,
        o.currency_code,
        o.channel_code,
        o.is_shipped,

        -- Customer context
        c.customer_name,
        c.customer_email,
        c.customer_status,
        c.region_code,

        -- Product context
        p.product_name,
        p.product_code,
        p.category_id,
        p.unit_price,

        -- Derived metrics
        datediff(o.ship_date, o.order_date)             as days_to_ship,
        case
            when o.total_amount >= p.unit_price * 2     then 'multi_unit'
            when o.total_amount >= p.unit_price         then 'single_unit'
            else 'below_unit_price'
        end                                             as order_size_category,

        -- Audit
        current_timestamp()                             as _dbt_loaded_at,
        '{{ invocation_id }}'                           as _dbt_invocation_id
    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join products p  on o.product_id  = p.product_id
)

select * from enriched
