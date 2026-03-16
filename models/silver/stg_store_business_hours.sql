with source as (
    select * from {{ source('raw', 'store_business_hours') }}
),

renamed as (
    select
        store_id,
        cast(weekday_iso as integer)                  as weekday_iso,
        -- Nombre del día a partir del ISO weekday (1=Lunes, 7=Domingo)
        case cast(weekday_iso as integer)
            when 1 then 'Lunes'
            when 2 then 'Martes'
            when 3 then 'Miércoles'
            when 4 then 'Jueves'
            when 5 then 'Viernes'
            when 6 then 'Sábado'
            when 7 then 'Domingo'
        end                                           as weekday_name,
        is_open,
        cast(open_time as varchar)                    as open_time,
        cast(close_time as varchar)                   as close_time
    from source
)

select * from renamed
