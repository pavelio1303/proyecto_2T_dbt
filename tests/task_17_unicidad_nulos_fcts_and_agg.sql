-- task_17_unicidad_nulos_fcts_and_agg
-- Objetivo: asegurar unicidad y no-nulos en claves de facts y agregados.
-- Criterio de fallo: devolver claves nulas o duplicadas.

WITH sales_nulls AS (
    SELECT
        'fct_sales.sale_item_id' AS failure_col,
        'NULL' AS failure_key
    FROM {{ ref('fct_sales') }}
    WHERE sale_item_id IS NULL
    LIMIT 1000
),
sales_dupes AS (
    SELECT
        'fct_sales.sale_item_id' AS failure_col,
        TO_VARCHAR(sale_item_id) AS failure_key
    FROM {{ ref('fct_sales') }}
    GROUP BY 1, 2
    HAVING COUNT(*) > 1
    LIMIT 1000
),

returns_nulls AS (
    SELECT
        'fct_returns.return_id' AS failure_col,
        'NULL' AS failure_key
    FROM {{ ref('fct_returns') }}
    WHERE return_id IS NULL
    LIMIT 1000
),
returns_dupes AS (
    SELECT
        'fct_returns.return_id' AS failure_col,
        TO_VARCHAR(return_id) AS failure_key
    FROM {{ ref('fct_returns') }}
    GROUP BY 1, 2
    HAVING COUNT(*) > 1
    LIMIT 1000
),

agg_nulls AS (
    SELECT
        'daily_store_performance.(sale_date,store_id)' AS failure_col,
        'NULL_KEY' AS failure_key
    FROM {{ ref('daily_store_performance') }}
    WHERE sale_date IS NULL OR store_id IS NULL
    LIMIT 1000
),
agg_dupes AS (
    SELECT
        'daily_store_performance.(sale_date,store_id)' AS failure_col,
        TO_VARCHAR(sale_date) || '-' || TO_VARCHAR(store_id) AS failure_key
    FROM {{ ref('daily_store_performance') }}
    GROUP BY 1, 2
    HAVING COUNT(*) > 1
    LIMIT 1000
),

failures AS (
    SELECT failure_col, failure_key FROM sales_nulls
    UNION ALL
    SELECT failure_col, failure_key FROM sales_dupes
    UNION ALL
    SELECT failure_col, failure_key FROM returns_nulls
    UNION ALL
    SELECT failure_col, failure_key FROM returns_dupes
    UNION ALL
    SELECT failure_col, failure_key FROM agg_nulls
    UNION ALL
    SELECT failure_col, failure_key FROM agg_dupes
)
SELECT failure_col, failure_key
FROM failures
LIMIT 1000

