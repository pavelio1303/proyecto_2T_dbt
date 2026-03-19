-- task_16_integridad_referencial_fct_sales_to_dims
-- Objetivo: garantizar que cada fila de `fct_sales` apunte a dimensiones válidas
-- (store, customer, product y date) a través de sus claves.
-- Criterio de fallo: devolver filas de `fct_sales` donde alguna dimensión no exista.

WITH fct AS (
    SELECT * FROM {{ ref('fct_sales') }}
),
joined AS (
    SELECT
        f.sale_item_id,
        f.sale_id,
        f.sale_date,
        f.store_id,
        f.customer_id,
        f.product_id,
        ds.store_id       AS dim_store_id,
        dc.customer_id    AS dim_customer_id,
        dp.product_id     AS dim_product_id,
        dd.date_day       AS dim_date_day
    FROM fct f
    LEFT JOIN {{ ref('dim_stores') }} ds
        ON f.store_id = ds.store_id
    LEFT JOIN {{ ref('dim_customers') }} dc
        ON f.customer_id = dc.customer_id
    LEFT JOIN {{ ref('dim_products') }} dp
        ON f.product_id = dp.product_id
    LEFT JOIN {{ ref('dim_date') }} dd
        ON f.sale_date = dd.date_day
)
SELECT
    sale_item_id,
    sale_id,
    sale_date,
    store_id,
    customer_id,
    product_id
FROM joined
WHERE
    -- Si customer_id es NULL, no es un error de integridad (no hay cliente identificado).
    dim_store_id IS NULL
    OR (customer_id IS NOT NULL AND dim_customer_id IS NULL)
    OR dim_product_id IS NULL
    OR dim_date_day IS NULL
LIMIT 1000

