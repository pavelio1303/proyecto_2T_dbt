{% snapshot inventory_status_snapshot %}

{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key="variant_id || '-' || store_id",
      check_cols=['stock_quantity'],
    )
}}

SELECT 
    variant_id,
    store_id,
    stock_quantity
FROM {{ ref('stg_stock') }}

{% endsnapshot %}