import psycopg2
import pandas as pd # type: ignore
#from display_converted_types import afficher_colonnes_converties
from collection_type_enum import collect_enum_columns, display_enum_conversion,convert_enum_to_check
from type_mapping import  convert_type



def extract_postgres_schema(connection_params):
    """
    Extrait l'ensemble des objets du schéma PostgreSQL (tables, colonnes, clés primaires, clés étrangères, index, vues et séquences).
    Affiche les résultats dans des tableaux en console.
    """
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    schema_data = {
        "tables": {},
        "views": {},
        "sequences": {}
    }

    # Extraction des tables
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
    """)
    tables = cursor.fetchall()

    for (table_name,) in tables:
        schema_data["tables"][table_name] = {}

        # Extraction des colonnes
        cursor.execute(f"""
            SELECT column_name, data_type, character_maximum_length, numeric_precision, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """)
        columns = cursor.fetchall()
        schema_data["tables"][table_name]["columns"] = columns

        # Afficher les colonnes en tableau
        columns_df = pd.DataFrame(
            columns,
            columns=["Nom colonne", "Type", "Longueur max", "Précision num", "Nullable"]
        )
        print(f"\nColonnes de la table {table_name} :")
        print(columns_df)

        # Extraction des clés primaires
        cursor.execute(f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = '{table_name}' AND tc.constraint_type = 'PRIMARY KEY'
        """)
        pk_columns = cursor.fetchall()
        schema_data["tables"][table_name]["primary_key"] = [col[0] for col in pk_columns]

        pk_df = pd.DataFrame(schema_data["tables"][table_name]["primary_key"], columns=["Colonnes clés primaires"])
        print(f"\nClé primaire de la table {table_name} :")
        print(pk_df)

        # Extraction des clés étrangères
        cursor.execute(f"""
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table,
                ccu.column_name AS foreign_column
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = '{table_name}';
        """)
        fk_constraints = cursor.fetchall()
        schema_data["tables"][table_name]["foreign_keys"] = [
            {
                "column": fk[0],
                "referenced_table": fk[1],
                "referenced_column": fk[2],
            }
            for fk in fk_constraints
        ]

        fk_df = pd.DataFrame(schema_data["tables"][table_name]["foreign_keys"])
        print(f"\nClés étrangères de la table {table_name} :")
        print(fk_df if not fk_df.empty else "Aucune")

        # Extraction des indexes
        cursor.execute(f"""
            SELECT
                i.relname AS index_name,
                a.attname AS column_name,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary
            FROM
                pg_class t,
                pg_class i,
                pg_index ix,
                pg_attribute a
            WHERE
                t.oid = ix.indrelid
                AND i.oid = ix.indexrelid
                AND a.attrelid = t.oid
                AND a.attnum = ANY(ix.indkey)
                AND t.relkind = 'r'
                AND t.relname = '{table_name}';
        """)
        indexes = cursor.fetchall()
        schema_data["tables"][table_name]["indexes"] = [
            {
                "index_name": idx[0],
                "column_name": idx[1],
                "is_unique": idx[2],
                "is_primary": idx[3],
            }
            for idx in indexes
        ]

        index_df = pd.DataFrame(schema_data["tables"][table_name]["indexes"])
        print(f"\nIndexes de la table {table_name} :")
        print(index_df if not index_df.empty else "Aucun")

    # Extraction des vues
    cursor.execute("""
        SELECT table_name, view_definition
        FROM information_schema.views
        WHERE table_schema = 'public'
    """)
    views = cursor.fetchall()
    schema_data["views"] = {view[0]: view[1] for view in views}

    views_df = pd.DataFrame(views, columns=["Nom vue", "Définition"])
    print("\nVues du schéma public :")
    print(views_df if not views_df.empty else "Aucune vue")

    # Extraction des séquences
    cursor.execute("""
        SELECT sequence_name
        FROM information_schema.sequences
        WHERE sequence_schema = 'public'
    """)
    sequences = cursor.fetchall()
    schema_data["sequences"] = [seq[0] for seq in sequences]

    seq_df = pd.DataFrame(schema_data["sequences"], columns=["Nom séquence"])
    print("\nSéquences du schéma public :")
    print(seq_df if not seq_df.empty else "Aucune séquence")
    
    #afficher_colonnes_converties(schema_data)
    
     # Extraction des colonnes ENUM
    print("Type ENUM existe schéma public :")
    enum_cols = collect_enum_columns(cursor)
    for col in enum_cols:
        print(f"Table: {col['table']}, Colonne: {col['column']}, ENUM: {col['enum_type']}, Valeurs: {col['enum_values']}")
  
    # Affichage des conversions Oracle
    display_enum_conversion(enum_cols)
    
    cursor.close()
    conn.close()

    return schema_data
