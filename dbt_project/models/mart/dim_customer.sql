-- dim_customer: Customer dimension table (SCD Type 1)
-- Migrated from SQL Server dbo.Customers
-- Materialization: table

{{ config(
    materialized='table',
    tags=['mart', 'analytics_ready', 'dimension']
) }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_order_stats as (
    select
        customer_id,
        count(distinct order_id)    as lifetime_orders,
        sum(total_amount)           as lifetime_revenue,
        max(order_date)             as last_order_date,
        min(order_date)             as first_order_date
    from {{ ref('stg_orders') }}
    where order_status not in ('CANCELLED', 'RETURNED')
    group by customer_id
),

dim as (
    select
        c.customer_id,
        c.customer_name,
        c.customer_email,
        c.customer_status,
        c.region_code,
        c.account_manager_id,
        c.created_date,
        c.updated_at,

        -- Enriched metrics
        coalesce(s.lifetime_orders, 0)              as lifetime_orders,
        coalesce(s.lifetime_revenue, 0.00)          as lifetime_revenue,
        s.last_order_date,
        s.first_order_date,

        -- Segment classification
        case
            when coalesce(s.lifetime_revenue, 0) >= 10000 then 'Gold'
            when coalesce(s.lifetime_revenue, 0) >= 1000  then 'Silver'
            else 'Bronze'
        end                                         as customer_segment,

        -- Audit
        current_timestamp()                         as _dbt_loaded_at,
        '{{ invocation_id }}'                       as _dbt_invocation_id
    from customers c
    left join customer_order_stats s on c.customer_id = s.customer_id
)

select * from dim
