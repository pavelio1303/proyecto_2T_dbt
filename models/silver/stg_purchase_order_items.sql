with source as (
    select * from {{ source('raw', 'purchase_order_items') }}
),

renamed as (
    select
        purchase_order_item_id,
        purchase_order_id,
        product_variant_id,
        cast(quantity_ordered as integer)               as quantity_ordered,
        cast(quantity_received as integer)              as quantity_received,
        -- Pendiente de recibir
        cast(quantity_ordered - quantity_received as integer) as quantity_pending,
        cast(unit_cost as number(12,2))                 as unit_cost,
        cast(quantity_ordered * unit_cost as number(12,2)) as total_cost_ordered,
        cast(quantity_received * unit_cost as number(12,2)) as total_cost_received,
        cast(created_at as timestamp)                   as created_at
    from source
)

select * from renamed
