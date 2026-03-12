with source as (
    select * from {{ source('raw', 'stock_movements') }}
),

renamed as (
    select
        stock_movement_id,
        store_id,
        product_variant_id,
        cast(movement_ts as timestamp)              as movement_ts,
        cast(movement_ts::date as date)             as movement_date,
        lower(trim(movement_type))                  as movement_type,
        -- Signo de cantidad: positivas = entrada, negativas = salida
        cast(quantity as integer)                   as quantity,
        cast(unit_cost as number(12,2))             as unit_cost,
        -- Coste total del movimiento (puede ser null si no hay unit_cost)
        case when unit_cost is not null
            then cast(abs(quantity) * unit_cost as number(12,2))
            else null
        end                                         as movement_cost,
        sale_item_id,
        return_item_id,
        purchase_order_item_id,
        trim(notes)                                 as notes,
        cast(created_at as timestamp)               as created_at
    from source
)

select * from renamed
