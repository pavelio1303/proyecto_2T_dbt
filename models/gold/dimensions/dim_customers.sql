WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
segmentation_params AS (
    -- Umbrales de negocio explícitos y reproducibles.
    SELECT
        2::NUMBER AS recurrent_orders_threshold,
        10::NUMBER AS vip_orders_threshold,
        1000::NUMBER(12,2) AS high_value_ltv_threshold,
        1500::NUMBER(12,2) AS vip_ltv_threshold
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
        COALESCE(s.total_orders, 0) AS total_orders,
        COALESCE(s.lifetime_value, 0) AS lifetime_value,
        -- Cliente recurrente: más de una compra histórica.
        (COALESCE(s.total_orders, 0) > 1) AS is_recurrent_customer,
        -- Segmentación recomendada para BI.
        CASE 
            WHEN COALESCE(s.total_orders, 0) >= p.vip_orders_threshold
                 OR COALESCE(s.lifetime_value, 0) >= p.vip_ltv_threshold THEN 'high_value'
            WHEN COALESCE(s.total_orders, 0) >= p.recurrent_orders_threshold THEN 'recurrent'
            ELSE 'new'
        END AS customer_segment,
        -- Flag VIP híbrido (frecuencia o gasto).
        (
            COALESCE(s.total_orders, 0) >= p.vip_orders_threshold
            OR COALESCE(s.lifetime_value, 0) >= p.vip_ltv_threshold
        ) AS is_vip,
        -- Flag auxiliar para identificar clientes de alto valor monetario.
        (COALESCE(s.lifetime_value, 0) >= p.high_value_ltv_threshold) AS is_high_value_customer
    FROM customers c
    CROSS JOIN segmentation_params p
    LEFT JOIN sales_summary s ON c.customer_id = s.customer_id
)
SELECT * FROM final