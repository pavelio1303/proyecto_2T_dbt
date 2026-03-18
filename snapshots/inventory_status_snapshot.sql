{% snapshot inventory_snapshot %}

{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key='inventory_id',
      check_cols=['quantity'],
    )
}}

-- Cambiamos STG_INVENTORY_STOCK por stg_inventory_stock
SELECT * FROM {{ ref('stg_inventory_stock') }}

{% endsnapshot %}