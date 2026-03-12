with source as (
    select * from {{ source('raw', 'refunds') }}
),

renamed as (
    select
        refund_id,
        return_id,
        cast(refund_ts as timestamp)                as refund_ts,
        cast(refund_ts::date as date)               as refund_date,
        lower(trim(refund_method))                  as refund_method,
        lower(trim(status))                         as status,
        cast(amount as number(12,2))                as amount,
        external_reference,
        cast(created_at as timestamp)               as created_at
    from source
)

select * from renamed
