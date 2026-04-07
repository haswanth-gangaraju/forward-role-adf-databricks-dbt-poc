-- stg_orders: Clean and standardise legacy order records from SQL Server
-- Source: legacy dbo.Orders table
-- Materialization: view

{{ config(materialized='view', tags=['staging', 'sql_server_migration']) }}

with source as (
    select
        OrderID,
        CustomerID,
        ProductID,
        OrderDate,
        ShipDate,
        OrderStatus,
        TotalAmount,
        CurrencyCode,
        SalesRepID,
        ChannelCode
    from {{ source('sql_server_dbo', 'Orders') }}
),

cleaned as (
    select
        OrderID                                             as order_id,
        CustomerID                                          as customer_id,
        ProductID                                           as product_id,
        cast(OrderDate as date)                             as order_date,
        cast(ShipDate as date)                              as ship_date,
        upper(trim(OrderStatus))                            as order_status,
        coalesce(cast(TotalAmount as decimal(18, 2)), 0.00) as total_amount,
        coalesce(upper(trim(CurrencyCode)), 'GBP')          as currency_code,
        SalesRepID                                          as sales_rep_id,
        upper(trim(ChannelCode))                            as channel_code,
        -- Derived
        case when ShipDate is not null then true else false end as is_shipped,
        -- Audit
        current_timestamp()                                 as _dbt_loaded_at,
        '{{ invocation_id }}'                               as _dbt_invocation_id
    from source
    where OrderID is not null
      and CustomerID is not null
)

select * from cleaned
