import psycopg2

conn = psycopg2.connect(
    host='vps-3a29d7f8.vps.ovh.net',
    port=5432,
    dbname='proyecto_dbt',
    user='alumno',
    password='1234'
)
cur = conn.cursor()

try:
    cur.execute("CREATE TABLE IF NOT EXISTS raw._test_perms (id int)")
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS raw._test_perms")
    conn.commit()
    print("OK: alumno PUEDE escribir en schema raw")
except Exception as e:
    print(f"ERROR: {e}")
    conn.rollback()
    # Intentar crear schema propio
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS landing")
        conn.commit()
        print("OK: alumno puede crear schema 'landing'")
        cur.execute("DROP SCHEMA IF EXISTS landing")
        conn.commit()
    except Exception as e2:
        print(f"ERROR schema propio: {e2}")

conn.close()
