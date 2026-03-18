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
        EXTRACT(dayofweek FROM date_day) AS day_of_week,
        -- Regla de negocio: Domingo es 0 en Snowflake usualmente
        CASE 
            WHEN EXTRACT(dayofweek FROM date_day) = 0 THEN 0 
            ELSE 1 
        END AS is_store_open_day
    FROM date_series
)

SELECT * FROM final