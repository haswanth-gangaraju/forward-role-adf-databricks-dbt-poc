-- fct_daily_sales: Daily sales fact table aggregated from enriched orders
-- Power BI reporting target — partitioned by order_date
-- Materialization: table

{{ config(
    materialized='table',
    tags=['mart', 'analytics_ready', 'power_bi'],
    partition_by={'field': 'order_date', 'data_type': 'date'}
) }}

with enriched_orders as (
    select * from {{ ref('int_order_enriched') }}
    where order_status not in ('CANCELLED', 'RETURNED')
),

daily_sales as (
    select
        order_date,
        region_code,
        channel_code,
        category_id,
        customer_status,

        -- Volume metrics
        count(distinct order_id)                            as order_count,
        count(distinct customer_id)                         as unique_customers,
        count(distinct product_id)                          as unique_products,

        -- Revenue metrics
        sum(total_amount)                                   as gross_revenue,
        avg(total_amount)                                   as avg_order_value,
        max(total_amount)                                   as max_order_value,
        min(total_amount)                                   as min_order_value,

        -- Fulfilment metrics
        sum(case when is_shipped then 1 else 0 end)         as shipped_orders,
        avg(days_to_ship)                                   as avg_days_to_ship,

        -- Audit
        current_timestamp()                                 as _dbt_loaded_at,
        '{{ invocation_id }}'                               as _dbt_invocation_id
    from enriched_orders
    group by
        order_date,
        region_code,
        channel_code,
        category_id,
        customer_status
)

select * from daily_sales
