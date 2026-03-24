WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),
brands AS (
    SELECT * FROM {{ ref('stg_brands') }}
),
categories AS (
    SELECT * FROM {{ ref('stg_categories') }}
),
variants AS (
    -- En dim_products no exponemos precios para evitar mezclar métricas por variante.
    SELECT
        DISTINCT product_id
    FROM {{ ref('stg_product_variants') }}
),
final AS (
    SELECT
        p.product_id,
        UPPER(TRIM(p.product_name)) AS product_name,
        UPPER(TRIM(b.brand_name)) AS brand_name,
        UPPER(TRIM(c.category_name)) AS category_name,
        UPPER(TRIM(COALESCE(p.model_name, p.product_name))) AS model_name,
        p.launch_date,
        -- Derivado: temporada aproximada según mes de lanzamiento.
        CASE
            WHEN MONTH(p.launch_date) BETWEEN 3 AND 8 THEN 'SPRING_SUMMER'
            ELSE 'FALL_WINTER'
        END AS season_derived,
        DATEDIFF('day', p.launch_date, CURRENT_DATE()) AS product_age_days,
        p.is_active AS is_product_active
    FROM products p
    LEFT JOIN variants v
        ON p.product_id = v.product_id
    LEFT JOIN brands b ON p.brand_id = b.brand_id
    LEFT JOIN categories c ON p.category_id = c.category_id
    WHERE v.product_id IS NOT NULL
)
SELECT * FROM final