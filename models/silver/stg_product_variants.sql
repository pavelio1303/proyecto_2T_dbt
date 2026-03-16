with source as (
    select * from {{ source('raw', 'product_variants') }}
),

renamed as (
    select
        product_variant_id,
        product_id,
        upper(trim(variant_sku))                      as variant_sku,
        trim(barcode)                                 as barcode,
        trim(color)                                   as color,
        cast(size_eu as decimal(4,1))                 as size_eu,
        cast(unit_cost as decimal(12,2))              as unit_cost,
        cast(list_price as decimal(12,2))             as list_price,
        -- Margen bruto unitario
        cast(list_price - unit_cost as decimal(12,2)) as gross_margin,
        -- % de margen sobre precio de venta
        case when list_price > 0
            then round((list_price - unit_cost) / list_price * 100, 2)
            else null
        end                                           as gross_margin_pct,
        is_active,
        cast(created_at as timestamp)                 as created_at,
        cast(updated_at as timestamp)                 as updated_at
    from source
)

select * from renamed
