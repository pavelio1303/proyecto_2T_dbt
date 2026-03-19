SELECT
    s.store_id,
    UPPER(TRIM(s.store_name)) AS store_name,
    UPPER(TRIM(s.city)) AS city,
    h.opening_time,
    h.closing_time
FROM {{ ref('stg_stores') }} s
LEFT JOIN {{ ref('stg_store_business_hours') }} h ON s.store_id = h.store_id