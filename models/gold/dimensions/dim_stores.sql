SELECT
    s.store_id,
    UPPER(TRIM(s.store_name)) AS store_name,
    UPPER(TRIM(s.city)) AS city,
    -- `stg_store_business_hours` tiene 1 fila por (store_id, weekday_iso).
    -- Para que `dim_stores` tenga grano 1 fila por `store_id`, agregamos.
    MIN(h.open_time) AS open_time,
    MAX(h.close_time) AS close_time
FROM {{ ref('stg_stores') }} s
LEFT JOIN {{ ref('stg_store_business_hours') }} h
    ON s.store_id = h.store_id
GROUP BY 1, 2, 3