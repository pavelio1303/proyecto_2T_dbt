SELECT
    r.return_id,
    r.sale_id,
    r.return_date,
    r.store_id,
    r.customer_id,
    UPPER(TRIM(r.reason)) AS return_reason,
    UPPER(TRIM(r.status)) AS return_status,
    r.refund_amount,
    -- Información del reembolso monetario
    rf.refund_method,
    rf.amount AS actual_cash_refunded
FROM {{ ref('stg_returns') }} r
LEFT JOIN {{ ref('stg_refunds') }} rf ON r.return_id = rf.return_id
WHERE r.status != 'CANCELLED'