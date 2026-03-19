with source as (
    select * from {{ source('raw', 'employees') }}
),

renamed as (
    select
        employee_id,
        upper(trim(employee_code))                    as employee_code,
        store_id,
        trim(first_name)                              as first_name,
        trim(last_name)                               as last_name,
        trim(first_name) || ' ' || trim(last_name)    as full_name,
        lower(trim(email))                            as email,
        phone,
        lower(trim(role))                             as role,
        cast(hire_date as date)                       as hire_date,
        cast(termination_date as date)                as termination_date,
        is_active,
        -- Empleado activo: sin fecha de baja y flag activo
        (is_active and termination_date is null)      as is_currently_active,
        cast(created_at as timestamp)                 as created_at
    from source
)

select * from renamed
