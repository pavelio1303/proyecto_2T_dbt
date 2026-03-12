with source as (
    select * from {{ source('raw', 'purchase_orders') }}
),

renamed as (
    select
        purchase_order_id,
        upper(trim(po_number))                          as po_number,
        supplier_id,
        destination_store_id,
        cast(order_ts as timestamp)                     as order_ts,
        cast(order_ts::date as date)                    as order_date,
        cast(expected_delivery_date as date)            as expected_delivery_date,
        cast(received_ts as timestamp)                  as received_ts,
        lower(trim(status))                             as status,
        -- Días de lead time (si ya está recibido)
        case when received_ts is not null
            then datediff('day', order_ts, received_ts)
            else null
        end                                             as lead_time_days,
        trim(notes)                                     as notes,
        cast(created_at as timestamp)                   as created_at,
        cast(updated_at as timestamp)                   as updated_at
    from source
)

select * from renamed
