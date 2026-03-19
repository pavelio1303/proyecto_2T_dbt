{{ config(
    materialized='incremental',
    unique_key='sale_item_id'
) }}

WITH sale_items AS (
    SELECT * FROM {{ ref('stg_sale_items') }}
),
-- Nota: Asegúrate de que el archivo en Silver se llame exactamente stg_sales.sql
sales_header AS (
    SELECT * FROM {{ ref('stg_sales') }} 
),
final AS (
    SELECT
        si.sale_item_id,
        si.sale_id,
        sh.sale_date,
        sh.store_id,
        sh.customer_id,
        si.product_variant_id AS variant_id,
        si.quantity,
        COALESCE(si.total_amount, (si.quantity * si.price)) AS gross_amount
    FROM sale_items si
    LEFT JOIN sales_header sh ON si.sale_id = sh.sale_id

    {% if is_incremental() %}
      WHERE sh.sale_date > (SELECT MAX(sale_date) FROM {{ this }})
    {% endif %}
)
SELECT * FROM final