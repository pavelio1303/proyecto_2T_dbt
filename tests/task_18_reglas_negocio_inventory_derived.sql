-- task_18_reglas_negocio_inventory_derived
-- Objetivo: validar reglas derivadas en `stg_inventory_stock`.
-- Criterio de fallo: devolver filas donde no se cumplan las relaciones derivadas.

WITH violations AS (
    SELECT
        store_id,
        product_variant_id,
        on_hand_qty,
        reserved_qty,
        available_qty,
        reorder_point,
        is_stockout,
        needs_reorder
    FROM {{ ref('stg_inventory_stock') }}
    WHERE
        available_qty <> (on_hand_qty - reserved_qty)
        OR is_stockout <> (available_qty <= 0)
        OR needs_reorder <> (available_qty <= reorder_point)
)
SELECT
    store_id,
    product_variant_id,
    available_qty,
    is_stockout,
    needs_reorder
FROM violations
LIMIT 1000

