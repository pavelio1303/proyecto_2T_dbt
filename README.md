# Sneaker Point Retail Analytics (dbt)

## Objetivo del proyecto
Sneaker Point es una cadena de tiendas físicas de zapatillas en España. El objetivo de este proyecto es construir una base analítica reutilizable para explotar el negocio con criterios reales de BI, incluyendo:

- análisis de ventas y devoluciones
- métricas por tienda y por día
- visión histórica del inventario (stock disponible)
- calidad y trazabilidad mediante tests y documentación (`dbt docs`)

## Arquitectura por capas

La arquitectura separa claramente:

- `RAW` (Landing): datos brutos ingeridos desde PostgreSQL en Snowflake
- `SILVER` (`stg_*`): limpieza, estandarización y columnas derivadas
- `GOLD`: dimensiones, hechos y agregados orientados a negocio
- `SNAPSHOTS`: histórico SCD (inventario)
- `incremental`: modelos incrementales (ventas en Silver)

```mermaid
flowchart TD
  A[PostgreSQL] -->|"dlt/ingesta -> CSVs"| B[Snowflake RAW (Landing)]
  B -->|"dbt models"| C[SILVER (stg_*)]
  C -->|"dbt models"| D[GOLD: Dimensiones + Hechos]
  C -->|"dbt snapshot"| E[SNAPSHOTS: inventory_snapshot]
  D -->|"agregaciones"| F[GOLD agregados: daily_store_performance]
```

## Ingesta (PostgreSQL -> CSV -> Snowflake)

Este proyecto extrae desde PostgreSQL y genera CSVs para cargarlos en Snowflake. El enunciado solicita dlt; en el repo, la ingesta operacional implementada es el script Python (extracción a CSVs) y la carga posterior en Snowflake.

### 1) Extraer a CSVs
Ejecuta:

```powershell
python ingesta\ingest.py
```

El script:

- conecta a PostgreSQL
- descarga las tablas listadas en `ingesta/ingest.py`
- escribe los CSVs en `ingesta/data/`

### 2) Preparar Snowflake y cargar RAW

Ejecuta `ingesta/setup_snowflake.sql` en Snowflake.

El SQL:

- crea `PROYECTO_2T` y los schemas `RAW`, `SILVER`, `GOLD` y `SNAPSHOTS`
- crea un `FILE FORMAT` y un stage interno `RAW.RAW_STAGE`
- define las tablas RAW
- ejecuta `COPY INTO` desde `@RAW.RAW_STAGE/...`

Importante: el archivo incluye instrucciones para `PUT` de los CSV en la sección 3. Antes de ejecutar `COPY INTO`, sube los CSV a:

- `@RAW.RAW_STAGE/<tabla>/`

## Ejecución de dbt

Una vez la capa RAW está cargada:

```powershell
dbt run
dbt snapshot
dbt test
dbt docs generate
```

> Nota: la conexión se configura vía `profiles.yml` (target `dev`) y los modelos se materializan según `dbt_project.yml`.
> En `dbt-fusion`, el equivalente a `dbt docs generate` suele ser `dbt man`.

## Modelos Gold principales

### Dimensiones Gold
- `dim_date`: calendario por día y bandera de día operativo
- `dim_stores`: metadatos de tienda con ventana horaria (open/close)
- `dim_customers`: perfil del cliente (órdenes, LTV y segmentación)
- `dim_products`: catálogo con marca/categoría y precio unitario derivado

### Hechos Gold
- `fct_sales`: ventas a nivel de línea (`sale_item_id`)
- `fct_returns`: devoluciones agregadas a nivel de `return_id`

### Agregado Gold
- `daily_store_performance`: KPIs diarios por tienda (ventas, devoluciones y net revenue)

## Snapshot e incremental elegidos

### Snapshot (histórico)
- Snapshot: `inventory_snapshot`
- Implementación: `snapshots/inventory_status_snapshot.sql`
- Estrategia: `check`
- `unique_key`: `inventory_id`
- Control del cambio: `check_cols = ['available_qty']`

La idea es conservar histórico del stock disponible por inventario (tienda + variante), permitiendo analizar roturas y disponibilidad a lo largo del tiempo.

### Incremental (Silver)
- Modelo incremental: `stg_sales` en Silver (`models/silver/stg_sales.sql`)
- Estrategia incremental: `materialized='incremental'`
- `unique_key`: `sale_id`
- Lógica: en incremental, filtra por `updated_at > max(updated_at)` de la tabla actual

## Decisiones de modelado en Gold

- **Granularidad de `fct_sales`**
  - `fct_sales` se modela a nivel `sale_item_id` (línea de venta), porque allí residen `quantity`, `unit_final_price` y el vínculo natural para asociar devoluciones por línea.
  - Enriquecimiento con la cabecera de venta (`stg_sales`) para `sale_date`, `store_id` y `customer_id`.
  - Mapeo variante -> producto uniendo `stg_product_variants` (deriva `product_id` desde `product_variant_id`).

- **Devoluciones y ventas netas**
  - Las devoluciones se agregan desde `stg_return_items` por `sale_item_id` (suma `line_refund_amount`).
  - En `fct_sales` se calculan:
    - `amount_refunded` (refunds asociados a esa línea)
    - `is_returned` (si `amount_refunded > 0`)
  - `daily_store_performance` calcula:
    - `gross_revenue` = suma de `quantity * unit_price`
    - `total_refunds` = suma de `amount_refunded`
    - `net_revenue` = `gross_revenue - total_refunds`

- **Métrica adicional: tasa de devolución**
  - `return_rate` en `daily_store_performance` se define como la proporción de líneas con devolución (`is_returned = TRUE`) sobre el total de líneas agregadas para cada `(sale_date, store_id)`.

## Uso del GPT cliente (Sneaker Point)

GPT cliente (técnico): https://chatgpt.com/g/g-69aef43c55a48191ab93c6f05c551410-cliente-tecnico-sneaker-point

Preguntas clave que hice y cómo impactaron decisiones de modelado:

- ¿Qué granularidad conviene para el hecho de ventas si necesitamos asociar devoluciones por línea?
  - Decisión: `fct_sales` a nivel `sale_item_id` para poder sumar refunds por línea.
- ¿Cómo tratar producto vs variante en el modelo de negocio?
  - Decisión: en `fct_sales` se mapea `product_variant_id -> product_id` (variante pertenece a un producto).
- ¿Cómo definir “venta neta” cuando existen devoluciones?
  - Decisión: `net_revenue` = gross revenue - refunds.
- ¿Qué métricas son razonables para un agregado diario por tienda?
  - Decisión: `daily_store_performance` expone transacciones, unidades, gross_revenue, total_refunds, net_revenue y `return_rate`.
- ¿Qué entidad histórica vale la pena snapshotear para análisis de stock?
  - Decisión: snapshot de inventario con `unique_key=inventory_id` y control sobre `available_qty`, para detectar cambios en disponibilidad.

<<<<<<< HEAD
=======
### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
- HOLA
>>>>>>> 37f3ef439b2ed3684381032e1fdf571bbfb7a0ec
