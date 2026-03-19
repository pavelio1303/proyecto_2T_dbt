WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),
brands AS (
    SELECT * FROM {{ ref('stg_brands') }}
),
categories AS (
    SELECT * FROM {{ ref('stg_categories') }}
),
final AS (
    SELECT
        p.product_id,
        UPPER(TRIM(p.product_name)) AS product_name,
        UPPER(TRIM(b.brand_name)) AS brand_name,
        UPPER(TRIM(c.category_name)) AS category_name,
        p.price AS unit_price
    FROM products p
    LEFT JOIN brands b ON p.brand_id = b.brand_id
    LEFT JOIN categories c ON p.category_id = c.category_id
)
SELECT * FROM final