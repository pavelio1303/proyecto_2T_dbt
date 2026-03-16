with source as (
    select * from {{ source('raw', 'customers') }}
),

renamed as (
    select
        customer_id,
        upper(trim(customer_code))                    as customer_code,
        trim(first_name)                              as first_name,
        trim(last_name)                               as last_name,
        trim(first_name) || ' ' || trim(last_name)    as full_name,
        lower(trim(email))                            as email,
        phone,
        cast(birth_date as date)                      as birth_date,
        -- Normalizar género
        case lower(trim(gender))
            when 'male'   then 'Hombre'
            when 'female' then 'Mujer'
            when 'm'      then 'Hombre'
            when 'f'      then 'Mujer'
            else coalesce(trim(gender), 'No especificado')
        end                                           as gender,
        trim(city)                                    as city,
        trim(province)                                as province,
        upper(trim(country_code))                     as country_code,
        signup_store_id,
        cast(signup_ts as timestamp)                  as signup_ts,
        lower(trim(status))                           as status,
        marketing_opt_in,
        cast(loyalty_points as integer)               as loyalty_points,
        cast(created_at as timestamp)                 as created_at,
        cast(updated_at as timestamp)                 as updated_at
    from source
)

select * from renamed
