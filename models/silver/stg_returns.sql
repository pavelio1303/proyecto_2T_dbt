with source as (
    select * from {{ source('raw', 'returns') }}
),

renamed as (
    select
        return_id,
        upper(trim(return_number))                  as return_number,
        sale_id,
        store_id,
        customer_id,
        processed_by_employee_id,
        cast(return_ts as timestamp)                as return_ts,
        cast(return_ts::date as date)               as return_date,
        lower(trim(status))                         as status,
        trim(reason)                                as reason,
        cast(refund_amount as number(12,2))         as refund_amount,
        cast(created_at as timestamp)               as created_at,
        cast(updated_at as timestamp)               as updated_at
    from source
)

select * from renamed
