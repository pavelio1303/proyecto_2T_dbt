{{ config(
    materialized='incremental',
    unique_key='stock_movement_id'
) }}

WITH movements AS (
    SELECT * FROM {{ ref('stg_stock_movements') }}
),

final AS (
    SELECT
        stock_movement_id,
        movement_ts,
        movement_date,
        store_id,
        product_variant_id,
        movement_type,
        quantity AS movement_qty_signed,
        ABS(quantity) AS movement_qty_abs,
        CASE WHEN quantity > 0 THEN quantity ELSE 0 END AS inbound_qty,
        CASE WHEN quantity < 0 THEN ABS(quantity) ELSE 0 END AS outbound_qty,
        unit_cost,
        movement_cost,
        sale_item_id,
        return_item_id,
        purchase_order_item_id,
        notes
    FROM movements
    {% if is_incremental() %}
      WHERE movement_ts > (SELECT MAX(movement_ts) FROM {{ this }})
    {% endif %}
)

SELECT * FROM final
