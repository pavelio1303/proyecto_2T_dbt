-- task_19_snapshot_inventory_has_history
-- Objetivo: si un `inventory_id` tiene múltiples filas (múltiples versiones), entonces debe existir historial (dbt_valid_to).
-- Criterio de fallo: existe algún `inventory_id` con COUNT(*) > 1, pero hist_rows = 0.

WITH multi_ids AS (
    SELECT inventory_id
    FROM {{ ref('inventory_snapshot') }}
    GROUP BY 1
    HAVING COUNT(*) > 1
    LIMIT 1
),
hist AS (
    SELECT COUNT(*) AS hist_rows
    FROM {{ ref('inventory_snapshot') }}
    WHERE dbt_valid_to IS NOT NULL
)
SELECT
    'inventory_snapshot_has_no_history_when_multi_versions_exist' AS failure
FROM multi_ids, hist
WHERE hist.hist_rows = 0
LIMIT 1

