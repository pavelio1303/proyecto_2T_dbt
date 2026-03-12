{% snapshot snap_customers %}
{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

/*
  Snapshot SCD Type 2 sobre clientes.

  Justificación funcional:
  - El status del cliente puede cambiar (active -> inactive -> banned).
  - Los loyalty_points se acumulan con cada compra.
  - La ciudad / provincia puede actualizarse si el cliente se muda.

  Mantener el histórico permite:
  - Analizar captación / pérdida de clientes en el tiempo.
  - Calcular KPIs de fidelización correctamente por periodo.
  - Auditar cambios sensibles (p.e. bans).
*/

select
    customer_id,
    customer_code,
    first_name,
    last_name,
    full_name,
    email,
    phone,
    gender,
    birth_date,
    city,
    province,
    country_code,
    signup_store_id,
    signup_ts,
    status,
    marketing_opt_in,
    loyalty_points,
    created_at,
    updated_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
