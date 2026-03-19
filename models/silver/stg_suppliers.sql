with source as (
    select * from {{ source('raw', 'suppliers') }}
),

renamed as (
    select
        supplier_id,
        upper(trim(supplier_code))                    as supplier_code,
        trim(supplier_name)                           as supplier_name,
        trim(contact_name)                            as contact_name,
        phone,
        lower(trim(email))                            as email,
        upper(trim(country_code))                     as country_code,
        is_active,
        cast(created_at as timestamp)                 as created_at
    from source
)

select * from renamed
