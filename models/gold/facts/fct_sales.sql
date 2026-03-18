{{ config(
    materialized='incremental',
    unique_key='sale_line_id'
) }}

WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
    {% if is_incremental() %}
      WHERE sale_date > (SELECT MAX(sale_date) FROM {{ this }})
    {% endif %}
),
returns_lookup AS (
    -- Agrupamos devoluciones por venta para evitar duplicar filas en el join
    SELECT 
        sale_id,
        COUNT(*) AS items_returned,
        SUM(refund_amount) AS total_refunded_amount
    FROM {{ ref('stg_returns') }}
    WHERE status = 'COMPLETED' -- Solo devoluciones procesadas
    GROUP BY 1
),
final AS (
    SELECT
        l.sale_line_id,
        s.sale_id,
        s.sale_date,
        s.store_id,
        s.customer_id,
        l.variant_id,
        l.quantity,
        l.unit_price,
        -- Indicador de si la venta fue devuelta
        CASE WHEN r.sale_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_returned,
        COALESCE(r.total_refunded_amount, 0) AS amount_refunded
    FROM {{ ref('stg_sale_lines') }} l
    INNER JOIN sales s ON l.sale_id = s.sale_id
    LEFT JOIN returns_lookup r ON s.sale_id = r.sale_id
)
SELECT * FROM final