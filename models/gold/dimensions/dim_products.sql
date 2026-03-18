WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),
variants AS (
    SELECT * FROM {{ ref('stg_variants') }}
),
final AS (
    SELECT
        v.variant_id,
        v.product_id,
        UPPER(TRIM(p.product_name)) AS product_name,
        UPPER(TRIM(p.brand_name)) AS brand,
        UPPER(TRIM(p.category_name)) AS category,
        v.size,
        v.color,
        v.unit_price,
        -- Atributo derivado: Gama de producto según precio
        CASE 
            WHEN v.unit_price > 150 THEN 'PREMIUM'
            WHEN v.unit_price BETWEEN 80 AND 150 THEN 'MID'
            ELSE 'BUDGET'
        END AS price_segment
    FROM variants v
    LEFT JOIN products p ON v.product_id = p.product_id
)
SELECT * FROM final