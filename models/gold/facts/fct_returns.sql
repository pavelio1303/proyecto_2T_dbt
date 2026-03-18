SELECT
    r.return_id,
    r.sale_id,
    ri.quantity,
    UPPER(TRIM(r.reason)) AS return_reason,
    UPPER(TRIM(r.status)) AS return_status,
    -- Usamos el monto de la tabla de devoluciones o del reembolso
    COALESCE(rf.amount, r.refund_amount) AS total_refunded_amount,
    rf.refund_date
FROM {{ ref('stg_returns') }} r
LEFT JOIN {{ ref('stg_return_items') }} ri ON r.return_id = ri.return_id
LEFT JOIN {{ ref('stg_refunds') }} rf ON r.return_id = rf.return_id -- OJO: a veces es rf.refund_id