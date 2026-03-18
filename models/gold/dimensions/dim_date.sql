-- Este bloque genera fechas desde 2024 hasta 2026 sin usar refs externos
WITH date_series AS (
    SELECT 
        generate_series(
            '2024-01-01'::date, 
            '2026-12-31'::date, 
            '1 day'::interval
        )::date AS date_day
)
SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    -- Lunes(1) a Sábado(6) Abierto, Domingo(0) Cerrado
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM date_day) = 0 THEN 0 
        ELSE 1 
    END AS is_store_open_day
FROM date_series