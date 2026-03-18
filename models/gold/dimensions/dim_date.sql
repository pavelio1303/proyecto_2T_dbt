-- Nota: Puedes usar dbt_utils.date_spine si lo tienes instalado
SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    -- Regla: Lunes(1) a Sábado(6) = Abierto (1), Domingo(0) = Cerrado (0)
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM date_day) = 0 THEN 0 
        ELSE 1 
    END AS is_store_open_day
FROM {{ ref('stg_dates') }}