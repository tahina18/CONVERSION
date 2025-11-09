import psycopg2
import oracledb
import json
from datetime import datetime

def convert_value_for_oracle(val):
    """
    Conversion simple de valeur de PostgreSQL vers format Oracle.
    Adaptez selon besoins spécifiques de mapping types.
    """
    if val is None:
        return None
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, (dict, list)):
        return json.dumps(val)  # convertir JSON en string
    if isinstance(val, datetime):
        return val  # datetime compatible
    return val

def migrate_table(pg_table, oracle_table, pg_conn, oracle_conn, pg_schema='public'):
    pg_cursor = pg_conn.cursor()
    oracle_cursor = oracle_conn.cursor()

    # Récupérer la liste des colonnes dans l'ordre du schéma PostgreSQL
    pg_cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name=%s AND table_schema=%s 
        ORDER BY ordinal_position
    """, (pg_table, pg_schema))
    columns = [row[0] for row in pg_cursor.fetchall()]

    # Création SQL dynamique pour SELECT et INSERT avec nommage exact et respect casse
    cols_pg = ', '.join([f'"{c}"' for c in columns])
    cols_oracle = ', '.join([f'"{c}"' for c in columns])

    select_sql = f'SELECT {cols_pg} FROM "{pg_schema}"."{pg_table}"'
    insert_sql = f'INSERT INTO "{oracle_table}" ({cols_oracle}) VALUES ({", ".join([":" + str(i+1) for i in range(len(columns))])})'

    pg_cursor.execute(select_sql)

    success_count = 0
    error_count = 0

    print(f"\nMigration table {pg_schema}.{pg_table} vers {oracle_table}")

    for idx, row in enumerate(pg_cursor.fetchall(), start=1):
        try:
            converted = tuple(convert_value_for_oracle(v) for v in row)
            oracle_cursor.execute(insert_sql, converted)
            oracle_conn.commit()
            success_count += 1
            if idx % 100 == 0:
                print(f"Lignes migrées: {idx}")
        except Exception as e:
            oracle_conn.rollback()
            error_count += 1
            print(f"\nErreur ligne {idx} : {e}")
            print(f"Données: {row}")

    print(f"\nFin migration {pg_schema}.{pg_table} : {success_count} lignes migrées, {error_count} erreurs\n")

    pg_cursor.close()
    oracle_cursor.close()

if __name__ == "__main__":
    PG_CONFIG = {
        'host': 'localhost',
        'port': 5432,
        'database': 'AURA',
        'user': 'postgres',
        'password': 'admin'
    }

    ORACLE_CONFIG = {
        'user': 'C##TEST',
        'password': 'admin',
        'dsn': 'localhost:1521/PROJET'
    }

    pg_conn = psycopg2.connect(**PG_CONFIG)
    oracle_conn = oracledb.connect(**ORACLE_CONFIG)

    # Spécifiez les tables avec schéma source PostgreSQL et nom cible Oracle
    tables = [
        ('ma_table_postgres', 'MA_TABLE_ORACLE', 'public'),
        # ('autre_table_postgres', 'AUTRE_TABLE_ORACLE', 'myschema'),
    ]

    for pg_table, oracle_table, pg_schema in tables:
        migrate_table(pg_table, oracle_table, pg_conn, oracle_conn, pg_schema)

    pg_conn.close()
    oracle_conn.close()
