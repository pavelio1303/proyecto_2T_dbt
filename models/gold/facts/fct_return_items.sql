{{ config(
    materialized='incremental',
    unique_key='return_item_id'
) }}

WITH return_items AS (
    SELECT * FROM {{ ref('stg_return_items') }}
),

returns_header AS (
    SELECT
        return_id,
        sale_id,
        store_id,
        customer_id,
        return_date,
        UPPER(TRIM(reason)) AS return_reason,
        UPPER(TRIM(status)) AS return_status
    FROM {{ ref('stg_returns') }}
),

final AS (
    SELECT
        ri.return_item_id,
        ri.return_id,
        rh.sale_id,
        rh.store_id,
        rh.customer_id,
        rh.return_date,
        rh.return_reason,
        rh.return_status,
        ri.sale_item_id,
        ri.quantity AS quantity_returned,
        ri.unit_refund_amount,
        ri.line_refund_amount
    FROM return_items ri
    LEFT JOIN returns_header rh
        ON ri.return_id = rh.return_id

    {% if is_incremental() %}
      WHERE rh.return_date > (SELECT MAX(return_date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM final
