-- task_16_integridad_referencial_fct_returns_to_fct_sales
-- Objetivo: garantizar que toda devolución (fct_returns) esté asociada a una venta existente (fct_sales).
-- Criterio de fallo: devolver devoluciones cuya `sale_id` no exista en `fct_sales.sale_id`.

WITH returns AS (
    SELECT return_id, sale_id
    FROM {{ ref('fct_returns') }}
),
sales AS (
    SELECT DISTINCT sale_id
    FROM {{ ref('fct_sales') }}
),
joined AS (
    SELECT
        r.return_id,
        r.sale_id
        , s.sale_id AS matched_sale_id
    FROM returns r
    LEFT JOIN sales s
        ON r.sale_id = s.sale_id
)
SELECT
    return_id,
    sale_id
FROM joined
WHERE sale_id IS NULL OR matched_sale_id IS NULL
LIMIT 1000

