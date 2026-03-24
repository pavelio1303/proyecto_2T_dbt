SELECT
    movement_date,
    store_id,
    product_variant_id,
    SUM(movement_qty_signed) AS net_movement_qty,
    SUM(inbound_qty) AS inbound_qty,
    SUM(outbound_qty) AS outbound_qty,
    COUNT(*) AS movements_count
FROM {{ ref('fct_stock_movements') }}
GROUP BY 1, 2, 3
