{{
    config(
        materialized = 'incremental',
        unique_key   = 'sale_id',
        on_schema_change = 'sync_all_columns'
    )
}}

with source as (
    select * from {{ source('raw', 'sales') }}

    {% if is_incremental() %}
        -- Solo cargamos filas nuevas o modificadas desde la última ejecución
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        sale_id,
        upper(trim(sale_number))                      as sale_number,
        store_id,
        customer_id,
        employee_id,
        cast(sale_ts as timestamp)                    as sale_ts,
        cast(sale_ts::date as date)                   as sale_date,
        date_part('hour', sale_ts)                    as sale_hour,
        date_part('dow', sale_ts)                     as sale_weekday_iso,    -- 0=Domingo, 6=Sábado (Snowflake)
        lower(trim(status))                           as status,
        cast(subtotal_amount as number(12,2))         as subtotal_amount,
        cast(discount_amount as number(12,2))         as discount_amount,
        cast(total_amount as number(12,2))            as total_amount,
        cast(items_count as integer)                  as items_count,
        -- Tasa de descuento sobre subtotal
        case when subtotal_amount > 0
            then round(discount_amount / subtotal_amount * 100, 2)
            else 0
        end                                           as discount_pct,
        -- Indicador de venta con descuento
        (discount_amount > 0)                         as has_discount,
        -- Indicador de venta con cliente identificado
        (customer_id is not null)                     as is_loyalty_sale,
        notes,
        cast(created_at as timestamp)                 as created_at,
        cast(updated_at as timestamp)                 as updated_at
    from source
)

select * from renamed
