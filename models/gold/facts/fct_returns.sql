WITH returns_header AS (
    SELECT
        return_id,
        sale_id,
        UPPER(TRIM(reason)) AS return_reason,
        UPPER(TRIM(status)) AS return_status,
        refund_amount AS header_refund_amount,
        -- Usamos fecha de devolución como fecha de referencia de la devolución
        return_date
    FROM {{ ref('stg_returns') }}
),

return_items_agg AS (
    SELECT
        return_id,
        SUM(quantity) AS quantity_returned,
        SUM(line_refund_amount) AS items_refunded_amount
    FROM {{ ref('stg_return_items') }}
    GROUP BY 1
),

refunds_agg AS (
    SELECT
        return_id,
        SUM(amount) AS refunds_amount,
        MAX(refund_date) AS last_refund_date
    FROM {{ ref('stg_refunds') }}
    GROUP BY 1
),

final AS (
    SELECT
        rh.return_id,
        rh.sale_id,
        ria.quantity_returned,
        rh.return_reason,
        rh.return_status,
        -- Preferimos el refund calculado en detalle de items; si no existe, usamos agregados de refunds o header.
        COALESCE(ria.items_refunded_amount, ra.refunds_amount, rh.header_refund_amount) AS total_refunded_amount,
        ra.last_refund_date AS refund_date,
        rh.return_date
    FROM returns_header rh
    LEFT JOIN return_items_agg ria
        ON rh.return_id = ria.return_id
    LEFT JOIN refunds_agg ra
        ON rh.return_id = ra.return_id
)

SELECT * FROM final