WITH stores AS (
    SELECT * FROM {{ ref('stg_stores') }}
),

final AS (
    SELECT
        store_id,
        -- Aplicando tus reglas de limpieza: UPPER y TRIM
        UPPER(TRIM(store_name)) AS store_name,
        UPPER(TRIM(city)) AS city,
        UPPER(TRIM(region)) AS region,
        
        -- Formateo de horarios (asumiendo que vienen de Silver)
        opening_hours,
        
        -- Atributo derivado: Tamaño de la tienda según número de empleados
        -- Esto ayuda a dirección a comparar tiendas de similar escala
        CASE 
            WHEN staff_count <= 5 THEN 'SMALL'
            WHEN staff_count BETWEEN 6 AND 15 THEN 'MEDIUM'
            ELSE 'LARGE'
        END AS store_size_segment,

        -- Información de contacto limpia
        LOWER(TRIM(manager_email)) AS manager_email,
        
        -- Fecha de apertura para calcular antigüedad
        opened_at
    FROM stores
)

SELECT * FROM final