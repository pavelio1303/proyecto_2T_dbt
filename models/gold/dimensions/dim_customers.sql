WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
sales_summary AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT sale_id) AS total_orders,
        SUM(total_amount) AS lifetime_value
    FROM {{ ref('stg_sales') }}
    GROUP BY 1
),
final AS (
    SELECT
        c.customer_id,
        UPPER(TRIM(c.first_name || ' ' || c.last_name)) AS full_name,
        UPPER(TRIM(c.email)) AS email,
        -- Mapeo de booleano según tus requisitos guardados
        CASE WHEN c.newsletter_status = 'S' THEN True ELSE False END AS is_subscriber,
        COALESCE(s.total_orders, 0) AS total_orders,
        COALESCE(s.lifetime_value, 0) AS lifetime_value,
        -- Segmentación dinámica
        CASE 
            WHEN s.lifetime_value > 1000 AND s.total_orders >= 5 THEN 'VIP'
            WHEN s.total_orders >= 1 THEN 'FRECUENTE'
            ELSE 'NUEVO'
        END AS customer_segment
    FROM customers c
    LEFT JOIN sales_summary s ON c.customer_id = s.customer_id
)
SELECT * FROM final