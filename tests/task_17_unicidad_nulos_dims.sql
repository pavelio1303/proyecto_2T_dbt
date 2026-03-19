-- task_17_unicidad_nulos_dims
-- Objetivo: asegurar unicidad y no-nulos para claves de dimensiones.
-- Criterio de fallo: devolver claves null o duplicadas.

WITH customer_nulls AS (
    SELECT
        'dim_customers.customer_id' AS failure_col,
        'NULL' AS failure_key
    FROM {{ ref('dim_customers') }}
    WHERE customer_id IS NULL
    LIMIT 1000
),
customer_dupes AS (
    SELECT
        'dim_customers.customer_id' AS failure_col,
        TO_VARCHAR(customer_id) AS failure_key
    FROM {{ ref('dim_customers') }}
    GROUP BY 1, 2
    HAVING COUNT(*) > 1
    LIMIT 1000
),

product_nulls AS (
    SELECT
        'dim_products.product_id' AS failure_col,
        'NULL' AS failure_key
    FROM {{ ref('dim_products') }}
    WHERE product_id IS NULL
    LIMIT 1000
),
product_dupes AS (
    SELECT
        'dim_products.product_id' AS failure_col,
        TO_VARCHAR(product_id) AS failure_key
    FROM {{ ref('dim_products') }}
    GROUP BY 1, 2
    HAVING COUNT(*) > 1
    LIMIT 1000
),

store_nulls AS (
    SELECT
        'dim_stores.store_id' AS failure_col,
        'NULL' AS failure_key
    FROM {{ ref('dim_stores') }}
    WHERE store_id IS NULL
    LIMIT 1000
),
store_dupes AS (
    SELECT
        'dim_stores.store_id' AS failure_col,
        TO_VARCHAR(store_id) AS failure_key
    FROM {{ ref('dim_stores') }}
    GROUP BY 1, 2
    HAVING COUNT(*) > 1
    LIMIT 1000
),

failures AS (
    SELECT failure_col, failure_key FROM customer_nulls
    UNION ALL
    SELECT failure_col, failure_key FROM customer_dupes
    UNION ALL
    SELECT failure_col, failure_key FROM product_nulls
    UNION ALL
    SELECT failure_col, failure_key FROM product_dupes
    UNION ALL
    SELECT failure_col, failure_key FROM store_nulls
    UNION ALL
    SELECT failure_col, failure_key FROM store_dupes
)
SELECT failure_col, failure_key
FROM failures
LIMIT 1000

