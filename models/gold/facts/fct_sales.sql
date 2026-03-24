{{ config(
    materialized='incremental',
    unique_key='sale_item_id',
    on_schema_change='sync_all_columns'
) }}

WITH sale_items AS (
    SELECT * FROM {{ ref('stg_sale_items') }}
),
-- Nota: Asegúrate de que el archivo en Silver se llame exactamente stg_sales.sql
sales_header AS (
    SELECT * FROM {{ ref('stg_sales') }} 
),

product_map AS (
    SELECT
        product_variant_id,
        product_id
    FROM {{ ref('stg_product_variants') }}
),

refunds_by_sale_item AS (
    -- Agregamos devoluciones por cada línea de venta (sale_item_id)
    SELECT
        sale_item_id,
        SUM(line_refund_amount) AS amount_refunded
    FROM {{ ref('stg_return_items') }}
    GROUP BY 1
),

final AS (
    SELECT
        si.sale_item_id,
        si.sale_id,
        sh.sale_date,
        sh.store_id,
        sh.customer_id,
        IFF(sh.customer_id IS NULL, 'WALKIN', 'IDENTIFIED') AS customer_type,
        pm.product_id,
        si.product_variant_id,
        si.quantity,
        -- Precio unitario final ya descontado (venta neta comercial por línea).
        si.unit_final_price AS unit_price,
        -- Precio unitario de lista (antes de descuento).
        si.unit_list_price,
        -- Descuento unitario aplicado.
        si.unit_discount_amount,
        COALESCE(r.amount_refunded, 0) AS amount_refunded,
        (COALESCE(r.amount_refunded, 0) > 0) AS is_returned,
        -- Importe de lista por línea (antes de descuentos y devoluciones).
        (si.quantity * si.unit_list_price) AS gross_amount,
        -- Descuento total de la línea.
        (si.quantity * si.unit_discount_amount) AS discount_amount,
        -- Venta neta comercial: venta bruta - descuentos (sin devoluciones).
        (si.quantity * si.unit_final_price) AS net_sales_before_returns,
        -- Venta neta real: venta neta comercial - devoluciones.
        ((si.quantity * si.unit_final_price) - COALESCE(r.amount_refunded, 0)) AS net_sales_after_returns
    FROM sale_items si
    LEFT JOIN sales_header sh
        ON si.sale_id = sh.sale_id
    LEFT JOIN product_map pm
        ON si.product_variant_id = pm.product_variant_id
    LEFT JOIN refunds_by_sale_item r
        ON si.sale_item_id = r.sale_item_id

    {% if is_incremental() %}
      WHERE sh.sale_date > (SELECT MAX(sale_date) FROM {{ this }})
    {% endif %}
)
SELECT * FROM final