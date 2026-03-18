SELECT
    s.sale_date,
    s.store_id,
    d.is_store_open_day,
    COUNT(DISTINCT s.sale_id) AS num_orders,
    SUM(s.quantity) AS total_items_sold,
    SUM(s.net_amount_before_tax) AS gross_revenue,
    SUM(s.discount_amount) AS total_discounts,
    -- Venta Neta final pedida por el cliente
    SUM(s.net_amount_before_tax) - SUM(s.discount_amount) AS final_net_revenue
FROM {{ ref('fct_sales') }} s
LEFT JOIN {{ ref('dim_date') }} d ON s.sale_date = d.date_day
GROUP BY 1, 2, 3