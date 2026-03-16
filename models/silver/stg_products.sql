with source as (
    select * from {{ source('raw', 'products') }}
),

renamed as (
    select
        product_id,
        upper(trim(product_code))                     as product_code,
        brand_id,
        category_id,
        trim(product_name)                            as product_name,
        trim(model_name)                              as model_name,
        trim(description)                             as description,
        -- Normalizar target_gender
        case lower(trim(target_gender))
            when 'male'    then 'Hombre'
            when 'female'  then 'Mujer'
            when 'unisex'  then 'Unisex'
            when 'kids'    then 'Niños'
            else coalesce(trim(target_gender), 'No definido')
        end                                           as target_gender,
        trim(sport_type)                              as sport_type,
        trim(material)                                as material,
        cast(launch_date as date)                     as launch_date,
        is_active,
        cast(created_at as timestamp)                 as created_at,
        cast(updated_at as timestamp)                 as updated_at
    from source
)

select * from renamed
