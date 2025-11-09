import psycopg2
import json

def extract_postgres_schema(connection_params, schema_name='public'):
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    
    schema_data = {}

    # Extraction des tables
    cursor.execute(f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_type='BASE TABLE'
    """, (schema_name,))
    tables = cursor.fetchall()

    for (table_name,) in tables:
        schema_data[table_name] = {}

        # Extraction des colonnes
        cursor.execute(f"""
            SELECT column_name, data_type, character_maximum_length, numeric_precision, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema_name, table_name))
        columns = cursor.fetchall()
        schema_data[table_name]['columns'] = columns

        # Extraction des contraintes de clé primaire
        cursor.execute(f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = %s AND tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
        """, (schema_name, table_name))
        pk_columns = cursor.fetchall()
        schema_data[table_name]['primary_key'] = pk_columns

    cursor.close()
    conn.close()

    return schema_data

# Exemple d'utilisation
if __name__ == "__main__":
    connection_params_pg = {
        'host': 'localhost',
        'dbname': 'AURA ',
        'user': 'postgres',
        'password': 'admin',
        'port': 5432
    }

    schema = extract_postgres_schema(connection_params_pg)

    # Sauvegarder dans un fichier JSON
    with open('extracted_schema.json', 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=4, ensure_ascii=False)

    print("Extraction du schéma terminée, sauvegardée dans extracted_schema.json")
