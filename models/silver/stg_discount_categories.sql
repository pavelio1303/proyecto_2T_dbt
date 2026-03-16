with source as (
    select * from {{ source('raw', 'discount_categories') }}
)

select
    discount_id,
    category_id
from source
