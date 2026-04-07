-- stg_customers: Clean and rename source SQL Server customer records
-- Source: legacy dbo.Customers table
-- Materialization: view

{{ config(materialized='view', tags=['staging', 'sql_server_migration']) }}

with source as (
    select
        CustomerID,
        CustomerName,
        CustomerEmail,
        CustomerStatus,
        CreatedDate,
        UpdatedDate,
        RegionCode,
        AccountManagerID
    from {{ source('sql_server_dbo', 'Customers') }}
),

renamed as (
    select
        CustomerID                              as customer_id,
        trim(CustomerName)                      as customer_name,
        lower(trim(CustomerEmail))              as customer_email,
        upper(trim(CustomerStatus))             as customer_status,
        cast(CreatedDate as date)               as created_date,
        cast(UpdatedDate as timestamp)          as updated_at,
        RegionCode                              as region_code,
        AccountManagerID                        as account_manager_id,
        -- Audit columns
        current_timestamp()                     as _dbt_loaded_at,
        '{{ invocation_id }}'                   as _dbt_invocation_id
    from source
    where CustomerID is not null
)

select * from renamed
