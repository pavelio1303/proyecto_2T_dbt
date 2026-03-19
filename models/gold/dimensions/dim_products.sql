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
    -- El precio está a nivel variante (producto + color + talla)
    SELECT
        product_id,
        list_price
    FROM {{ ref('stg_product_variants') }}
),
final AS (
    SELECT
        p.product_id,
        UPPER(TRIM(p.product_name)) AS product_name,
        UPPER(TRIM(b.brand_name)) AS brand_name,
        UPPER(TRIM(c.category_name)) AS category_name,
        AVG(v.list_price) AS unit_price
    FROM products p
    LEFT JOIN brands b ON p.brand_id = b.brand_id
    LEFT JOIN categories c ON p.category_id = c.category_id
    LEFT JOIN variants v ON p.product_id = v.product_id
    GROUP BY 1, 2, 3, 4
)
SELECT * FROM final