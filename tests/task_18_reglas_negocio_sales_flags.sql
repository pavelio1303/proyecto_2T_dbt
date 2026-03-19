-- task_18_reglas_negocio_sales_flags
-- Objetivo: validar flags y métricas derivadas en `stg_sales`.
-- Criterio de fallo: devolver filas donde las derivaciones no coincidan con el cálculo esperado.

WITH s AS (
    SELECT * FROM {{ ref('stg_sales') }}
)
SELECT
    sale_id,
    discount_pct,
    discount_amount,
    subtotal_amount,
    has_discount,
    customer_id,
    is_loyalty_sale
FROM s
WHERE
    discount_pct <> (
        CASE
            WHEN subtotal_amount > 0
                THEN ROUND(discount_amount / subtotal_amount * 100, 2)
            ELSE 0
        END
    )
    OR has_discount <> (discount_amount > 0)
    OR is_loyalty_sale <> (customer_id IS NOT NULL)
LIMIT 1000

