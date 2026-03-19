-- task_19_snapshot_inventory_available_qty_changed
-- Objetivo: comprobar que existe al menos un inventario cuya `available_qty` cambie entre versiones.
-- Criterio de fallo: si existe algún `inventory_id` con múltiples filas (múltiples versiones) y no hay ningún cambio de available_qty.

WITH ordered AS (
    SELECT
        inventory_id,
        available_qty,
        dbt_valid_from,
        LAG(available_qty) OVER (
            PARTITION BY inventory_id
            ORDER BY dbt_valid_from
        ) AS prev_available_qty
    FROM {{ ref('inventory_snapshot') }}
),
changed AS (
    SELECT DISTINCT inventory_id
    FROM ordered
    WHERE prev_available_qty IS NOT NULL
      AND available_qty <> prev_available_qty
),
counts AS (
    SELECT COUNT(DISTINCT inventory_id) AS changed_inventories
    FROM changed
),
multi_ids AS (
    SELECT inventory_id
    FROM {{ ref('inventory_snapshot') }}
    GROUP BY 1
    HAVING COUNT(*) > 1
    LIMIT 1
)
SELECT
    'inventory_snapshot_has_no_available_qty_changes' AS failure
FROM counts, multi_ids
WHERE counts.changed_inventories = 0
LIMIT 1

