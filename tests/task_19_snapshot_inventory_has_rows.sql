-- task_19_snapshot_inventory_has_rows
-- Objetivo: verificar que el snapshot `inventory_snapshot` no esté vacío.
-- Criterio de fallo: devolver una fila si el snapshot no tiene registros.

WITH counts AS (
    SELECT COUNT(*) AS snapshot_rows
    FROM {{ ref('inventory_snapshot') }}
)
SELECT
    'inventory_snapshot_is_empty' AS failure
FROM counts
WHERE snapshot_rows = 0
LIMIT 1

