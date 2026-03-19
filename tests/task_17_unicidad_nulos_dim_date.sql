-- task_17_unicidad_nulos_dim_date
-- Objetivo: asegurar que `dim_date.date_day` no contenga nulos y que sea único.
-- Criterio de fallo: devolver fechas nulas o duplicadas.

WITH null_fail AS (
    SELECT
        'dim_date.date_day' AS failure_col,
        'NULL' AS failure_key
    FROM {{ ref('dim_date') }}
    WHERE date_day IS NULL
    LIMIT 1000
),
dupe_fail AS (
    SELECT
        'dim_date.date_day' AS failure_col,
        TO_VARCHAR(date_day) AS failure_key
    FROM {{ ref('dim_date') }}
    GROUP BY 1, 2
    HAVING COUNT(*) > 1
    LIMIT 1000
)
SELECT failure_col, failure_key
FROM (
    SELECT * FROM null_fail
    UNION ALL
    SELECT * FROM dupe_fail
) x
LIMIT 1000

