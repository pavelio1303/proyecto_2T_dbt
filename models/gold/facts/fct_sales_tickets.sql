{{ config(
    materialized='table'
) }}

-- Fact derivada a nivel ticket (1 fila = 1 sale_id).
WITH sales_lines AS (
    SELECT * FROM {{ ref('fct_sales') }}
),

final AS (
    SELECT
        sale_id,
        MAX(sale_date) AS sale_date,
        MAX(store_id) AS store_id,
        MAX(customer_id) AS customer_id,
        MAX(customer_type) AS customer_type,
        SUM(quantity) AS total_units_sold,
        SUM(gross_amount) AS gross_sales_amount,
        SUM(discount_amount) AS discount_amount,
        SUM(net_sales_before_returns) AS net_sales_before_returns,
        SUM(amount_refunded) AS refunded_amount,
        SUM(net_sales_after_returns) AS net_sales_after_returns
    FROM sales_lines
    GROUP BY 1
)

SELECT * FROM final
