import psycopg2

def collect_sequences_with_current_value(cursor):
    """
    Collecte les séquences de la base PostgreSQL et récupère leurs métadonnées ainsi que la dernière valeur utilisée.
    
    :param cursor: un curseur psycopg2 pour exécuter des requêtes sur PostgreSQL
    :return: une liste de dictionnaires, chacun contenant les informations d'une séquence
    """
    # Exécute une requête pour récupérer les noms et bornes des séquences dans le schéma 'public'
    cursor.execute("""
        SELECT sequence_name, start_value, minimum_value, maximum_value
        FROM information_schema.sequences
        WHERE sequence_schema = 'public'
    """)
    rows = cursor.fetchall()  

    sequences = []
    for row in rows:
        seq_name = row[0]          
        start_value = row[1]       
        min_value = row[2]        
        max_value = row[3]         

        # Récupère la dernière valeur générée par la séquence via une requête dynamique
        cursor.execute(f'SELECT last_value FROM "{seq_name}"')
        current_value = cursor.fetchone()[0]  

        # Crée un dictionnaire contenant toutes les informations pertinentes
        seq = {
            "name": seq_name,
            "start_value": start_value,
            "minimum_value": min_value,
            "maximum_value": max_value,
            "current_value": current_value
        }
        sequences.append(seq)

        # Affiche les informations pour vérification
        print(f"Sequence: {seq_name}, Start: {start_value}, Min: {min_value}, Max: {max_value}, Current: {current_value}")

    return sequences
#collecter le colonne de type serial
def collect_serial_columns(cursor):
    """
    Collecte les colonnes des tables PostgreSQL qui ont un type serial,
    c'est-à-dire les colonnes avec un default value faisant référence à une séquence.
    
    :param cursor: curseur psycopg2 déjà connecté à la base PostgreSQL
    :return: liste de dictionnaires contenant schema, table, colonne et séquence associée
    """
    query = """
    SELECT 
        table_schema,
        table_name,
        column_name,
        column_default
    FROM 
        information_schema.columns
    WHERE 
        column_default LIKE 'nextval(%' 
        AND table_schema = 'public'
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    serial_columns = []
    for row in rows:
        table_schema, table_name, column_name, column_default = row

        # Extraire le nom de la séquence depuis la fonction nextval, ex : nextval('seq_name'::regclass)
        seq_name_start = column_default.find("'") + 1
        seq_name_end = column_default.find("'", seq_name_start)
        sequence_name = column_default[seq_name_start:seq_name_end]

        serial_columns.append({
            "schema": table_schema,
            "table": table_name,
            "column": column_name,
            "sequence": sequence_name
        })

        print(f"Table: {table_name}.{column_name} uses sequence {sequence_name}")

    return serial_columns

#collection de sequences manuel ou automatique
import psycopg2

def collect_sequences_auto_or_manual(cursor):
    """
    Récupère toutes les séquences dans PostgreSQL et indique pour chacune 
    si elle est automatique (propriétaire d'une colonne serial) ou manuelle.
    Retourne aussi le nombre total de séquences automatiques et manuelles.
    
    :param cursor: curseur psycopg2 connecté à la base PostgreSQL
    :return: tuple (séquences, total_auto, total_manual)
             où séquences est la liste des séquences récupérées
    """
    query = """
    SELECT
      nsp.nspname AS sequence_schema,
      seq.relname AS sequence_name,
      tbl.relname AS table_name,
      attr.attname AS column_name,
      TRUE AS is_auto_created
    FROM pg_class seq
    JOIN pg_namespace nsp ON seq.relnamespace = nsp.oid
    JOIN pg_depend dep ON dep.objid = seq.oid
    JOIN pg_class tbl ON dep.refobjid = tbl.oid
    JOIN pg_attribute attr ON attr.attrelid = tbl.oid AND attr.attnum = dep.refobjsubid
    WHERE seq.relkind = 'S'  -- sequence type
    AND dep.deptype = 'a'

    UNION

    SELECT
      nsp.nspname AS sequence_schema,
      seq.relname AS sequence_name,
      NULL AS table_name,
      NULL AS column_name,
      FALSE AS is_auto_created
    FROM pg_class seq
    JOIN pg_namespace nsp ON seq.relnamespace = nsp.oid
    WHERE seq.relkind = 'S'
    AND seq.oid NOT IN (
      SELECT objid FROM pg_depend WHERE deptype = 'a'
    )
    ORDER BY sequence_schema, sequence_name;
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    sequences = []
    total_auto = 0
    total_manual = 0

    for row in rows:
        seq_schema, seq_name, tbl_name, col_name, is_auto = row
        seq_type = "automatic" if is_auto else "manual"  # Conversion booléen en chaîne
        sequences.append({
            "sequence_schema": seq_schema,
            "sequence_name": seq_name,
            "table_name": tbl_name,
            "column_name": col_name,
            "type": seq_type
        })
        if is_auto:
            total_auto += 1
        else:
            total_manual += 1

    print(f"Total sequences automatic: {total_auto}")
    print(f"Total sequences manual: {total_manual}")

    return sequences, total_auto, total_manual


#collecte des colonne de type serial
def collect_serial_columns_with_count(cursor):
    query = """
    SELECT 
      table_name,
      column_name
    FROM information_schema.columns
    WHERE column_default LIKE 'nextval(%'
      AND table_schema = 'public'
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    serial_columns = []
    for table_name, column_name in rows:
        serial_columns.append({
            "table": table_name,
            "column": column_name
        })
        print(f"Table: {table_name}, Column: {column_name} is serial")
    
    total = len(serial_columns)
    print(f"Total serial columns: {total}")
    
    return serial_columns, total

#fonction pour obtenir le type de sequence
def get_sequence_types(connection_params):
    """
    Se connecte à la base PostgreSQL, récupère toutes les séquences,
    indique pour chacune si elle est automatique (owned by colonne serial)
    ou manuelle.

    :param connection_params: dictionnaire de paramètres de connexion PostgreSQL
    :return: liste des séquences avec type (automatic/manual)
    """
    query = """
    SELECT
      nsp.nspname AS sequence_schema,
      seq.relname AS sequence_name,
      tbl.relname AS table_name,
      attr.attname AS column_name,
      TRUE AS is_auto_created
    FROM pg_class seq
    JOIN pg_namespace nsp ON seq.relnamespace = nsp.oid
    JOIN pg_depend dep ON dep.objid = seq.oid
    JOIN pg_class tbl ON dep.refobjid = tbl.oid
    JOIN pg_attribute attr ON attr.attrelid = tbl.oid AND attr.attnum = dep.refobjsubid
    WHERE seq.relkind = 'S'  -- sequence type
    AND dep.deptype = 'a'

    UNION

    SELECT
      nsp.nspname AS sequence_schema,
      seq.relname AS sequence_name,
      NULL AS table_name,
      NULL AS column_name,
      FALSE AS is_auto_created
    FROM pg_class seq
    JOIN pg_namespace nsp ON seq.relnamespace = nsp.oid
    WHERE seq.relkind = 'S'
    AND seq.oid NOT IN (
      SELECT objid FROM pg_depend WHERE deptype = 'a'
    )
    ORDER BY sequence_schema, sequence_name;
    """

    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    sequences = []
    for seq_schema, seq_name, tbl_name, col_name, is_auto in rows:
        seq_type = "automatic" if is_auto else "manual"
        sequences.append({
            "schema": seq_schema,
            "sequence_name": seq_name,
            "table_name": tbl_name,
            "column_name": col_name,
            "type": seq_type
        })

    return sequences




