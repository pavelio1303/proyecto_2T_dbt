with source as (
    select * from {{ source('raw', 'sale_items') }}
),

renamed as (
    select
        sale_item_id,
        sale_id,
        product_variant_id,
        cast(quantity as integer)                                 as quantity,
        cast(unit_list_price as number(12,2))                     as unit_list_price,
        cast(unit_discount_amount as number(12,2))                as unit_discount_amount,
        cast(unit_final_price as number(12,2))                    as unit_final_price,
        cast(line_subtotal as number(12,2))                       as line_subtotal,
        cast(line_discount_total as number(12,2))                 as line_discount_total,
        cast(line_total as number(12,2))                          as line_total,
        -- Descuento porcentual sobre precio de lista
        case when unit_list_price > 0
            then round(unit_discount_amount / unit_list_price * 100, 2)
            else 0
        end                                                       as unit_discount_pct,
        -- Flag: línea con descuento
        (unit_discount_amount > 0)                                as has_discount,
        cast(created_at as timestamp)                             as created_at
    from source
)

select * from renamed
