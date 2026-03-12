with source as (
    select * from {{ source('raw', 'discounts') }}
),

renamed as (
    select
        discount_id,
        upper(trim(discount_code))                    as discount_code,
        trim(discount_name)                           as discount_name,
        lower(trim(discount_type))                    as discount_type,
        lower(trim(scope))                            as scope,
        cast(value_amount as decimal(12,2))           as value_amount,
        cast(min_ticket_amount as decimal(12,2))      as min_ticket_amount,
        cast(max_discount_amount as decimal(12,2))    as max_discount_amount,
        cast(start_ts as timestamp)                   as start_ts,
        cast(end_ts as timestamp)                     as end_ts,
        is_stackable,
        is_active,
        cast(created_at as timestamp)                 as created_at
    from source
)

select * from renamed
