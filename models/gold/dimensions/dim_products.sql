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
        -- Si estas columnas dan error, es que se llaman distinto en stg_products
        -- Puedes probar con p.product_price o p.amount
        p.price AS unit_price,
        CASE 
            WHEN p.price > 150 THEN 'PREMIUM'
            WHEN p.price BETWEEN 80 AND 150 THEN 'MID'
            ELSE 'BUDGET'
        END AS price_segment
    FROM products p
    LEFT JOIN brands b ON p.brand_id = b.brand_id
    LEFT JOIN categories c ON p.category_id = c.category_id
)
SELECT * FROM final