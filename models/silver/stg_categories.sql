with source as (
    select * from {{ source('raw', 'categories') }}
),

renamed as (
    select
        category_id,
        parent_category_id,
        trim(category_name)                           as category_name,
        upper(trim(category_code))                    as category_code,
        -- Es categoría raíz si no tiene padre
        (parent_category_id is null)                  as is_root_category,
        cast(created_at as timestamp)                 as created_at
    from source
)

select * from renamed
