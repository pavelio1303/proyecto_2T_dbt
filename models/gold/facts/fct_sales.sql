{{ config(
    materialized='incremental',
    unique_key='sale_line_id',
    on_schema_change='fail'
) }}

WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
    {% if is_incremental() %}
      -- Solo traemos datos nuevos desde la última carga
      WHERE sale_date > (SELECT MAX(sale_date) FROM {{ this }})
    {% endif %}
),
sale_lines AS (
    SELECT * FROM {{ ref('stg_sale_lines') }}
),
final AS (
    SELECT
        l.sale_line_id,
        s.sale_id,
        s.sale_date,
        s.store_id,
        s.customer_id,
        l.variant_id,
        l.quantity,
        l.unit_price AS price_at_sale,
        l.discount_amount,
        -- Cálculo de base imponible e IVA (asumiendo 21%)
        (l.quantity * l.unit_price) - l.discount_amount AS net_amount_before_tax,
        ((l.quantity * l.unit_price) - l.discount_amount) * 0.21 AS tax_amount
    FROM sale_lines l
    INNER JOIN sales s ON l.sale_id = s.sale_id
)
SELECT * FROM final