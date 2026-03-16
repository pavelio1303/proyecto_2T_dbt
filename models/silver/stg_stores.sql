with source as (
    select * from {{ source('raw', 'stores') }}
),

renamed as (
    select
        store_id,
        upper(trim(store_code))                       as store_code,
        trim(store_name)                              as store_name,
        trim(address_line_1)                          as address_line_1,
        trim(address_line_2)                          as address_line_2,
        trim(city)                                    as city,
        trim(province)                                as province,
        trim(postal_code)                             as postal_code,
        upper(trim(country_code))                     as country_code,
        phone,
        lower(trim(email))                            as email,
        cast(opened_on as date)                       as opened_on,
        cast(closed_on as date)                       as closed_on,
        timezone_name,
        lower(trim(status))                           as status,
        -- La tienda está activa si status = 'active' y no tiene fecha de cierre
        (lower(trim(status)) = 'active' and closed_on is null) as is_active,
        cast(created_at as timestamp)                 as created_at,
        cast(updated_at as timestamp)                 as updated_at
    from source
)

select * from renamed
