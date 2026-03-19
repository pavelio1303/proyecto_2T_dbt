with source as (
    select * from {{ source('raw', 'sale_item_discounts') }}
)

select
    sale_item_discount_id,
    sale_item_id,
    discount_id,
    cast(discount_amount as number(12,2))   as discount_amount,
    cast(created_at as timestamp)           as created_at
from source
