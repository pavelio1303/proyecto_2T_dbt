with source as (
    select * from {{ source('raw', 'return_items') }}
),

renamed as (
    select
        return_item_id,
        return_id,
        sale_item_id,
        cast(quantity as integer)                           as quantity,
        cast(unit_refund_amount as number(12,2))            as unit_refund_amount,
        cast(line_refund_amount as number(12,2))            as line_refund_amount,
        trim(return_reason)                                 as return_reason,
        trim(condition_notes)                               as condition_notes,
        cast(created_at as timestamp)                       as created_at
    from source
)

select * from renamed
