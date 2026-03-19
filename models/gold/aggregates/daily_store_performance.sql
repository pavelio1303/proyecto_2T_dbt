SELECT
    s.sale_date,
    s.store_id,
    COUNT(DISTINCT s.sale_id) AS total_transactions,
    SUM(s.quantity) AS total_units_sold,
    -- Venta Bruta
    SUM(s.quantity * s.unit_price) AS gross_revenue,
    -- Impacto de Devoluciones
    SUM(s.amount_refunded) AS total_refunds,
    -- VENTA NETA FINAL (Requisito GPT Cliente)
    SUM(s.quantity * s.unit_price) - SUM(s.amount_refunded) AS net_revenue,
    -- Tasa de devolución (KPI extra para nota)
    DIV0(SUM(IFF(s.is_returned, 1, 0)), COUNT(*)) AS return_rate
FROM {{ ref('fct_sales') }} s
GROUP BY 1, 2