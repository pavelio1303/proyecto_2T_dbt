SELECT
    s.sale_date,
    s.store_id,
    COUNT(DISTINCT s.sale_id) AS total_transactions,
    SUM(s.quantity) AS total_units_sold,
    -- Venta bruta (precio lista).
    SUM(s.gross_amount) AS gross_sales_amount,
    -- Venta neta comercial (sin devoluciones).
    SUM(s.net_sales_before_returns) AS net_sales_before_returns,
    -- Devoluciones.
    SUM(s.amount_refunded) AS refunded_amount,
    -- Venta neta real (post-devoluciones).
    SUM(s.net_sales_after_returns) AS net_sales_after_returns,
    -- Tasa de devolución por importe.
    DIV0(SUM(s.amount_refunded), SUM(s.net_sales_before_returns)) AS return_rate_amount
FROM {{ ref('fct_sales') }} s
GROUP BY 1, 2