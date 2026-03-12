with source as (
    select * from {{ source('raw', 'payments') }}
),

renamed as (
    select
        payment_id,
        sale_id,
        cast(payment_ts as timestamp)               as payment_ts,
        cast(payment_ts::date as date)              as payment_date,
        lower(trim(payment_method))                 as payment_method,
        lower(trim(status))                         as status,
        cast(amount as number(12,2))                as amount,
        external_reference,
        cast(created_at as timestamp)               as created_at
    from source
)

select * from renamed
