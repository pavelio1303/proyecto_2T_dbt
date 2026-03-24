WITH date_series AS (
    SELECT 
        DATEADD(day, seq4(), '2024-01-01') AS date_day
    FROM TABLE(GENERATOR(rowcount => 1095)) -- Genera 3 años de fechas
),

final AS (
    SELECT
        date_day,
        EXTRACT(year FROM date_day) AS year,
        EXTRACT(month FROM date_day) AS month,
        EXTRACT(day FROM date_day) AS day,
        EXTRACT(week FROM date_day) AS week,
        EXTRACT(dayofweek FROM date_day) AS day_of_week,
        DAYNAME(date_day) AS day_name,
        (EXTRACT(dayofweek FROM date_day) IN (0, 6)) AS is_weekend,
        (EXTRACT(dayofweek FROM date_day) = 0) AS is_sunday,
        -- Regla cliente: domingo no operativo (sin calendario de festivos por ahora).
        (EXTRACT(dayofweek FROM date_day) <> 0) AS is_business_day,
        -- Alias legacy para compatibilidad con reportes existentes.
        IFF(EXTRACT(dayofweek FROM date_day) = 0, 0, 1) AS is_store_open_day
    FROM date_series
)

SELECT * FROM final