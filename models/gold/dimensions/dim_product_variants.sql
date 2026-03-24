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
    SELECT * FROM {{ ref('stg_product_variants') }}
),
final AS (
    SELECT
        v.product_variant_id,
        v.product_id,
        UPPER(TRIM(p.product_name)) AS product_name,
        UPPER(TRIM(COALESCE(p.model_name, p.product_name))) AS model_name,
        UPPER(TRIM(b.brand_name)) AS brand_name,
        UPPER(TRIM(c.category_name)) AS category_name,
        UPPER(TRIM(COALESCE(v.color, 'N/A'))) AS color,
        v.size_eu,
        v.variant_sku,
        p.launch_date,
        CASE
            WHEN MONTH(p.launch_date) BETWEEN 3 AND 8 THEN 'SPRING_SUMMER'
            ELSE 'FALL_WINTER'
        END AS season_derived,
        CASE
            WHEN v.list_price < 80 THEN 'LOW'
            WHEN v.list_price < 140 THEN 'MID'
            ELSE 'PREMIUM'
        END AS price_segment,
        DATEDIFF('day', p.launch_date, CURRENT_DATE()) AS product_age_days,
        v.list_price AS unit_price,
        v.unit_cost,
        v.gross_margin,
        v.gross_margin_pct,
        p.is_active AS is_product_active,
        v.is_active AS is_variant_active
    FROM variants v
    INNER JOIN products p
        ON v.product_id = p.product_id
    LEFT JOIN brands b
        ON p.brand_id = b.brand_id
    LEFT JOIN categories c
        ON p.category_id = c.category_id
)
SELECT * FROM final
