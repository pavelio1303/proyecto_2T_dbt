with source as (
    select * from {{ source('raw', 'inventory_stock') }}
),

renamed as (
    select
        store_id,
        product_variant_id,
        -- Identificador estable de inventario por (tienda, variante).
        -- Se usa en snapshots para detectar cambios de stock a lo largo del tiempo.
        store_id || '-' || product_variant_id as inventory_id,
        cast(on_hand_qty as integer)                  as on_hand_qty,
        cast(reserved_qty as integer)                 as reserved_qty,
        -- Stock disponible real = en mano - reservado
        cast(on_hand_qty - reserved_qty as integer)   as available_qty,
        cast(reorder_point as integer)                as reorder_point,
        cast(reorder_qty as integer)                  as reorder_qty,
        -- Indicador de rotura de stock (disponible <= 0)
        (on_hand_qty - reserved_qty <= 0)             as is_stockout,
        -- Indicador de reposición necesaria
        ((on_hand_qty - reserved_qty) <= reorder_point) as needs_reorder,
        cast(last_counted_at as timestamp)            as last_counted_at,
        cast(updated_at as timestamp)                 as updated_at
    from source
)

select * from renamed
