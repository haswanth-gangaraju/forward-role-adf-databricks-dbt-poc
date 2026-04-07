-- stg_products: Clean product master data from SQL Server
-- Source: legacy dbo.Products table
-- Materialization: view

{{ config(materialized='view', tags=['staging', 'sql_server_migration']) }}

with source as (
    select
        ProductID,
        ProductName,
        ProductCode,
        CategoryID,
        UnitPrice,
        StockQuantity,
        IsActive,
        CreatedDate
    from {{ source('sql_server_dbo', 'Products') }}
),

cleaned as (
    select
        ProductID                                               as product_id,
        trim(ProductName)                                       as product_name,
        upper(trim(ProductCode))                                as product_code,
        CategoryID                                              as category_id,
        cast(coalesce(UnitPrice, 0) as decimal(18, 2))          as unit_price,
        coalesce(cast(StockQuantity as int), 0)                 as stock_quantity,
        case
            when IsActive = 1 or upper(cast(IsActive as string)) = 'TRUE' then true
            else false
        end                                                     as is_active,
        cast(CreatedDate as date)                               as created_date,
        current_timestamp()                                     as _dbt_loaded_at,
        '{{ invocation_id }}'                                   as _dbt_invocation_id
    from source
    where ProductID is not null
)

select * from cleaned
