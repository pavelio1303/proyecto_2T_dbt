with source as (
    select * from {{ source('raw', 'brands') }}
),

renamed as (
    select
        brand_id,
        trim(brand_name)                              as brand_name,
        upper(trim(country_code))                     as country_code,
        cast(created_at as timestamp)                 as created_at
    from source
)

select * from renamed
