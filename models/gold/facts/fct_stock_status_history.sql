WITH snapshot_rows AS (
    SELECT * FROM {{ ref('inventory_snapshot') }}
),

final AS (
    SELECT
        inventory_id,
        store_id,
        product_variant_id,
        available_qty,
        reorder_point,
        needs_reorder,
        (available_qty <= 0) AS is_stockout,
        dbt_valid_from AS valid_from,
        dbt_valid_to AS valid_to
    FROM snapshot_rows
)

SELECT * FROM final
