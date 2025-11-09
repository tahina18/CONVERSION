import psycopg2

PG_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'AURA',
    'user': 'postgres',
    'password': 'admin'
}

def get_tables(cursor):
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    return [row[0] for row in cursor.fetchall()]

def get_not_null_columns(cursor, table):
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND is_nullable = 'NO';
    """, (table,))
    return [row[0] for row in cursor.fetchall()]

def check_nulls(cursor, table, not_null_columns):
    if not not_null_columns:
        return False, []
    nulls_found = False
    columns_with_nulls = []
    for col in not_null_columns:
        query = f'SELECT COUNT(*) FROM "{table}" WHERE "{col}" IS NULL'
        cursor.execute(query)
        count = cursor.fetchone()[0]
        if count > 0:
            nulls_found = True
            columns_with_nulls.append((col, count))
    return nulls_found, columns_with_nulls

def main():
    conn = psycopg2.connect(**PG_CONFIG)
    cursor = conn.cursor()
    print("Vérification des valeurs NULL dans les colonnes NOT NULL...\n")
    tables = get_tables(cursor)
    errors_found = False
    for table in tables:
        nn_cols = get_not_null_columns(cursor, table)
        has_null, null_cols = check_nulls(cursor, table, nn_cols)
        if has_null:
            errors_found = True
            print(f"Table '{table}':")
            for col, count in null_cols:
                print(f"  - {count} valeur(s) NULL trouvée(s) dans colonne NOT NULL '{col}'.")
            print()
    if not errors_found:
        print("Aucune valeur NULL détectée dans les colonnes NOT NULL.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
