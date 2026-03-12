with source as (
    select * from {{ source('raw', 'discount_stores') }}
)

select
    discount_id,
    store_id
from source
