-- task_18_reglas_negocio_store_business_hours_weekday
-- Objetivo: validar el mapeo exacto de `weekday_iso` a `weekday_name` en `stg_store_business_hours`.
-- Criterio de fallo: devolver filas donde el nombre no coincide con el ISO.

SELECT
    store_id,
    weekday_iso,
    weekday_name,
    is_open
FROM {{ ref('stg_store_business_hours') }}
WHERE
    (weekday_iso = 1 AND weekday_name <> 'Lunes')
    OR (weekday_iso = 2 AND weekday_name <> 'Martes')
    OR (weekday_iso = 3 AND weekday_name <> 'Miércoles')
    OR (weekday_iso = 4 AND weekday_name <> 'Jueves')
    OR (weekday_iso = 5 AND weekday_name <> 'Viernes')
    OR (weekday_iso = 6 AND weekday_name <> 'Sábado')
    OR (weekday_iso = 7 AND weekday_name <> 'Domingo')
    OR weekday_name IS NULL
LIMIT 1000

