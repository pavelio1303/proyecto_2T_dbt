import csv
import os
import psycopg2

# ── Conexión fuente PostgreSQL ────────────────────────────────────────────────
PG_CONFIG = dict(
    host="vps-3a29d7f8.vps.ovh.net",
    port=5432,
    dbname="proyecto_dbt",
    user="alumno",
    password="1234",
)

# ── Tablas a extraer (schema retail) ─────────────────────────────────────────
TABLES = [
    "brands",
    "categories",
    "stores",
    "store_business_hours",
    "employees",
    "suppliers",
    "products",
    "product_variants",
    "customers",
    "discounts",
    "discount_categories",
    "discount_customers",
    "discount_products",
    "discount_stores",
    "inventory_stock",
    "sales",
    "sale_items",
    "sale_item_discounts",
    "payments",
    "returns",
    "return_items",
    "refunds",
    "stock_movements",
    "purchase_orders",
    "purchase_order_items",
]

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")


def extract_table_to_csv(cur, table_name: str, output_dir: str) -> int:
    """Extrae una tabla completa a CSV. Devuelve el número de filas."""
    cur.execute(f'SELECT * FROM retail."{table_name}"')
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]

    filepath = os.path.join(output_dir, f"{table_name}.csv")
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(col_names)
        writer.writerows(rows)

    return len(rows)


def run_ingesta():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Conectando a PostgreSQL...")
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    print(f"Extrayendo {len(TABLES)} tablas hacia: {OUTPUT_DIR}\n")
    total_rows = 0
    for table in TABLES:
        n = extract_table_to_csv(cur, table, OUTPUT_DIR)
        total_rows += n
        print(f"  OK  {table:<35s}  {n:>6} filas  ->  {table}.csv")

    cur.close()
    conn.close()

    print(f"\nExtraccion completada. Total filas: {total_rows:,}")
    print(f"Archivos CSV en: {OUTPUT_DIR}")
    print("\nProximos pasos:")
    print("  1. Sube los CSV a tu Snowflake Internal Stage (@RAW_STAGE).")
    print("  2. Ejecuta ingesta/setup_snowflake.sql en Snowflake.")


if __name__ == "__main__":
    run_ingesta()
