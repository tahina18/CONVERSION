import psycopg2
import oracledb
import pandas as pd

def fetch_table_data_pg(conn, table_name, schema='public'):
    query = f'SELECT * FROM "{schema}"."{table_name}"'
    return pd.read_sql_query(query, conn)

def fetch_table_data_oracle(conn, table_name):
    query = f'SELECT * FROM "{table_name}"'
    return pd.read_sql_query(query, conn)

def compare_dataframes(df_src, df_tgt, key_columns):
    # Même code de comparaison que précédemment
    ...

if __name__ == '__main__':
    pg_conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='AURA',
        user='postgres',
        password='admin'
    )

    oracle_conn = oracledb.connect(
        user='C##TEST',
        password='admin',
        dsn='localhost:1521/PROJET'
    )

    # Mettez ici vos véritables noms de tables et schéma
    table_pg = 'nom_exact_table_postgres'
    schema_pg = 'public'  # ou autre si vous avez un autre schéma
    table_oracle = 'NOM_EXACT_TABLE_ORACLE'

    print(f"Chargement données de {schema_pg}.{table_pg} (PostgreSQL)...")
    df_pg = fetch_table_data_pg(pg_conn, table_pg, schema_pg)

    print(f"Chargement données de {table_oracle} (Oracle)...")
    df_oracle = fetch_table_data_oracle(oracle_conn, table_oracle)

    key_cols = ['votre_cle_primaire']  # Exemple : ['id']

    print("Comparaison des données...")
    compare_dataframes(df_pg, df_oracle, key_cols)

    pg_conn.close()
    oracle_conn.close()
