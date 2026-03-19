-- task_18_reglas_negocio_stock_movements_sign
-- Objetivo: asegurar la convención de signos de `quantity` en `stg_stock_movements`.
-- Criterio de fallo: devolver movimientos cuyo `movement_type` contiene cantidades con signo mixto
-- (hay a la vez valores > 0 y < 0 dentro del mismo tipo).

WITH stats AS (
    SELECT
        movement_type,
        MIN(quantity) AS min_qty,
        MAX(quantity) AS max_qty
    FROM {{ ref('stg_stock_movements') }}
    GROUP BY 1
),
mixed_types AS (
    -- Si min_qty < 0 y max_qty > 0, entonces el dataset mezcla signos para ese movement_type.
    SELECT movement_type
    FROM stats
    WHERE min_qty < 0 AND max_qty > 0
)
SELECT
    sm.stock_movement_id,
    sm.store_id,
    sm.product_variant_id,
    sm.movement_type,
    sm.quantity
FROM {{ ref('stg_stock_movements') }} sm
JOIN mixed_types mt
    ON sm.movement_type = mt.movement_type
LIMIT 1000

