"""
Module collection_indexes.py
----------------------------
Ce module contient toutes les fonctions pour la collecte, l'analyse, 
la manipulation et la conversion des index PostgreSQL vers Oracle,
avec analyse intelligente de la cardinalité pour les index BITMAP.
"""

import psycopg2


def collect_postgresql_indexes(cursor):
    """
    Récupère tous les index PostgreSQL avec leurs détails et affiche les résultats.
    
    :param cursor: curseur psycopg2 connecté à PostgreSQL
    :return: liste des index récupérés
    """
    query = """
    SELECT 
        i.schemaname AS schema_name,
        i.tablename AS table_name,
        i.indexname AS index_name,
        i.indexdef AS index_definition,
        am.amname AS index_type,
        array_agg(a.attname ORDER BY a.attnum) AS indexed_columns
    FROM pg_indexes i
    JOIN pg_class c ON c.relname = i.indexname
    JOIN pg_index idx ON idx.indexrelid = c.oid
    JOIN pg_class t ON t.oid = idx.indrelid
    JOIN pg_am am ON am.oid = c.relam
    LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(idx.indkey)
    WHERE i.schemaname = 'public'
    GROUP BY i.schemaname, i.tablename, i.indexname, i.indexdef, am.amname
    ORDER BY i.tablename, i.indexname;
    """
    
    cursor.execute(query)
    indexes = cursor.fetchall()
    
    # Affichage des résultats
    print(f"\n{'='*80}")
    print(f"COLLECTE DES INDEX POSTGRESQL")
    print(f"{'='*80}\n")
    
    if not indexes:
        print("Aucun index trouvé dans le schéma 'public'.\n")
        return indexes
    
    print(f"Nombre total d'index trouvés : {len(indexes)}\n")
    
    for idx, index in enumerate(indexes, 1):
        schema, table, index_name, index_def, index_type, columns = index
        
        print(f"--- INDEX #{idx} ---")
        print(f"Nom de l'index      : {index_name}")
        print(f"Table               : {table}")
        print(f"Type                : {index_type}")
        print(f"Colonnes            : {', '.join(columns) if columns else 'N/A'}")
        print(f"Définition          : {index_def}")
        print()
    
    print(f"{'='*80}\n")
    
    return indexes


def analyze_indexes(indexes):
    """
    Analyse les index et les catégorise en index automatiques (contraintes)
    et index manuels créés par l'utilisateur.
    
    :param indexes: liste des index récupérés de PostgreSQL
    :return: dictionnaire avec deux catégories d'index
    """
    primary_key_indexes = []
    unique_indexes = []
    foreign_key_indexes = []
    manual_indexes = []
    
    for index in indexes:
        schema, table, index_name, index_def, index_type, columns = index
        
        # Détection des index automatiques créés par les contraintes
        if '_pkey' in index_name:
            primary_key_indexes.append({
                'name': index_name,
                'table': table,
                'type': 'primary_key',
                'columns': columns,
                'index_type': index_type,
                'definition': index_def
            })
        elif '_key' in index_name or 'UNIQUE' in index_def.upper():
            unique_indexes.append({
                'name': index_name,
                'table': table,
                'type': 'unique',
                'columns': columns,
                'index_type': index_type,
                'definition': index_def
            })
        elif '_fkey' in index_name:
            foreign_key_indexes.append({
                'name': index_name,
                'table': table,
                'type': 'foreign_key',
                'columns': columns,
                'index_type': index_type,
                'definition': index_def
            })
        else:
            manual_indexes.append({
                'name': index_name,
                'table': table,
                'type': 'manual',
                'columns': columns,
                'index_type': index_type,
                'definition': index_def
            })
    
    # Affichage de l'analyse
    print(f"\n{'='*80}")
    print(f"ANALYSE DES INDEX")
    print(f"{'='*80}\n")
    
    if primary_key_indexes:
        print(f"🔑 {len(primary_key_indexes)} index de clé primaire détecté(s):")
        for idx in primary_key_indexes:
            print(f"   - {idx['name']} sur table {idx['table']}")
        print("   → Ces index seront créés automatiquement avec les contraintes PRIMARY KEY\n")
    
    if unique_indexes:
        print(f"🔒 {len(unique_indexes)} index unique détecté(s):")
        for idx in unique_indexes:
            print(f"   - {idx['name']} sur table {idx['table']}")
        print("   → Ces index seront créés automatiquement avec les contraintes UNIQUE\n")
    
    if foreign_key_indexes:
        print(f"🔗 {len(foreign_key_indexes)} index de clé étrangère détecté(s):")
        for idx in foreign_key_indexes:
            print(f"   - {idx['name']} sur table {idx['table']}")
        print("   → Ces index peuvent nécessiter une création manuelle en Oracle\n")
    
    if manual_indexes:
        print(f"📊 {len(manual_indexes)} index manuel(s) détecté(s):")
        for idx in manual_indexes:
            print(f"   - {idx['name']} sur table {idx['table']} (type: {idx['index_type']})")
        print("   → Ces index doivent être convertis en Oracle\n")
    
    print(f"{'='*80}\n")
    
    return {
        'primary_key': primary_key_indexes,
        'unique': unique_indexes,
        'foreign_key': foreign_key_indexes,
        'manual': manual_indexes
    }


def analyze_column_cardinality(cursor, table_name, column_name):
    """
    Analyse la cardinalité d'une colonne pour déterminer si un index BITMAP est approprié.
    
    :param cursor: curseur psycopg2
    :param table_name: nom de la table
    :param column_name: nom de la colonne
    :return: tuple (total_rows, distinct_values, cardinality_ratio)
    """
    try:
        # Récupérer le nombre total de lignes
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = cursor.fetchone()[0]
        
        # Récupérer le nombre de valeurs distinctes
        cursor.execute(f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name}")
        distinct_values = cursor.fetchone()[0]
        
        # Calculer le ratio de cardinalité
        if total_rows > 0:
            cardinality_ratio = distinct_values / total_rows
        else:
            cardinality_ratio = 1.0
        
        return total_rows, distinct_values, cardinality_ratio
    except Exception as e:
        print(f"⚠ Erreur analyse cardinalité pour {table_name}.{column_name}: {e}")
        return 0, 0, 1.0


def should_use_bitmap_index(cursor, table_name, columns):
    """
    Détermine si un index BITMAP est approprié basé sur la cardinalité.
    
    Règle générale :
    - Cardinalité < 5% : BITMAP recommandé
    - Cardinalité > 20% : BTREE recommandé
    - Entre 5% et 20% : Analyser au cas par cas
    
    :param cursor: curseur psycopg2
    :param table_name: nom de la table
    :param columns: liste des colonnes indexées
    :return: tuple (should_use_bitmap, recommendation_message)
    """
    if not columns or len(columns) == 0:
        return False, "Colonnes non définies, utilisation BTREE par défaut"
    
    # Analyser la première colonne (pour simplifier)
    column_name = columns[0] if isinstance(columns, list) else columns
    
    try:
        total_rows, distinct_values, cardinality_ratio = analyze_column_cardinality(
            cursor, table_name, column_name
        )
        
        # Règles de décision
        if cardinality_ratio < 0.05:  # Moins de 5% de valeurs distinctes
            return True, f"BITMAP recommandé (cardinalité: {cardinality_ratio:.2%}, {distinct_values} valeurs distinctes sur {total_rows} lignes)"
        elif cardinality_ratio > 0.20:  # Plus de 20% de valeurs distinctes
            return False, f"BTREE recommandé (cardinalité: {cardinality_ratio:.2%}, {distinct_values} valeurs distinctes sur {total_rows} lignes)"
        else:
            return False, f"BTREE par défaut (cardinalité: {cardinality_ratio:.2%}, analyse manuelle recommandée)"
    
    except Exception as e:
        return False, f"Erreur d'analyse: {str(e)}, utilisation BTREE par défaut"


def convert_index_type_with_analysis(cursor, pg_index_type, table_name, columns):
    """
    Convertit le type d'index PostgreSQL vers Oracle avec analyse intelligente.
    
    :param cursor: curseur psycopg2
    :param pg_index_type: type d'index PostgreSQL
    :param table_name: nom de la table
    :param columns: colonnes indexées
    :return: tuple (oracle_type, recommendation)
    """
    # Pour les index GIN (généralement sur colonnes à faible cardinalité)
    if pg_index_type.lower() == 'gin':
        use_bitmap, message = should_use_bitmap_index(cursor, table_name, columns)
        if use_bitmap:
            return 'BITMAP', message
        else:
            return 'BTREE', message
    
    # Pour les autres types, utiliser le mapping standard
    index_type_mapping = {
        'btree': ('BTREE', 'Type standard Oracle (B-tree)'),
        'hash': ('BTREE', 'Hash non supporté en Oracle, conversion en BTREE'),
        'gist': ('BTREE', 'GiST converti en BTREE (recherche spatiale non supportée directement)'),
        'spgist': ('BTREE', 'SP-GiST converti en BTREE'),
        'brin': ('BTREE', 'BRIN converti en BTREE (Block Range Index non supporté)')
    }
    
    oracle_type, message = index_type_mapping.get(pg_index_type.lower(), ('BTREE', 'Type inconnu, utilisation BTREE par défaut'))
    return oracle_type, message


def generate_oracle_index_ddl_smart(cursor, index_name, table_name, columns, pg_index_type, unique=False):
    """
    Génère le DDL Oracle pour un index avec analyse intelligente.
    
    :param cursor: curseur psycopg2
    :param index_name: nom de l'index
    :param table_name: nom de la table
    :param columns: liste des colonnes indexées
    :param pg_index_type: type d'index PostgreSQL
    :param unique: True si l'index est unique
    :return: tuple (ddl, recommendation)
    """
    # Analyse intelligente du type d'index
    oracle_index_type, recommendation = convert_index_type_with_analysis(
        cursor, pg_index_type, table_name, columns
    )
    
    unique_clause = "UNIQUE " if unique else ""
    columns_str = ", ".join(columns) if isinstance(columns, list) else columns
    
    # Les index BITMAP ne peuvent pas être UNIQUE
    if oracle_index_type == 'BITMAP' and unique:
        oracle_index_type = 'BTREE'
        recommendation += " (UNIQUE index converti en BTREE car BITMAP ne supporte pas UNIQUE)"
    
    # Génération du DDL
    if oracle_index_type == 'BITMAP':
        ddl = f"CREATE BITMAP INDEX {index_name}\n"
        ddl += f"  ON {table_name} ({columns_str});\n"
    else:
        ddl = f"CREATE {unique_clause}INDEX {index_name}\n"
        ddl += f"  ON {table_name} ({columns_str});\n"
    
    return ddl, recommendation


def generate_indexes_ddl(connection_params):
    """
    Fonction principale pour générer les DDL Oracle des index avec analyse intelligente.
    Collecte, analyse et convertit tous les index PostgreSQL.
    
    :param connection_params: paramètres de connexion PostgreSQL
    """
    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cursor:
            # Étape 1 : Collecter les index
            indexes = collect_postgresql_indexes(cursor)
            
            if not indexes:
                print("✓ Aucun index à traiter.\n")
                return
            
            # Étape 2 : Analyser et catégoriser
            indexes_data = analyze_indexes(indexes)
            
            # Étape 3 : Gérer les index automatiques
            auto_indexes = (
                indexes_data.get('primary_key', []) + 
                indexes_data.get('unique', [])
            )
            
            if auto_indexes:
                print(f"\n{'='*80}")
                print("GESTION DES INDEX AUTOMATIQUES")
                print(f"{'='*80}\n")
                
                for idx in auto_indexes:
                    print(f"❌ Index '{idx['name']}' sur table '{idx['table']}'")
                    print(f"   → NE PAS CRÉER : sera créé automatiquement avec la contrainte {idx['type'].upper()}\n")
            
            # Étape 4 : Convertir les index manuels et de clés étrangères
            manual_indexes = (
                indexes_data.get('manual', []) + 
                indexes_data.get('foreign_key', [])
            )
            
            if manual_indexes:
                print(f"\n{'='*80}")
                print("CONVERSION DES INDEX EN ORACLE (avec analyse intelligente)")
                print(f"{'='*80}\n")
                
                for idx in manual_indexes:
                    # Déterminer si l'index est unique
                    is_unique = 'UNIQUE' in idx['definition'].upper()
                    
                    # Génération avec analyse intelligente
                    oracle_ddl, recommendation = generate_oracle_index_ddl_smart(
                        cursor,
                        idx['name'],
                        idx['table'],
                        idx['columns'],
                        idx['index_type'],
                        is_unique
                    )
                    
                    print(f"-- Index PostgreSQL: {idx['name']}")
                    print(f"-- Table: {idx['table']}")
                    print(f"-- Type PostgreSQL: {idx['index_type']}")
                    print(f"-- Colonnes: {', '.join(idx['columns'])}")
                    print(f"-- Recommandation: {recommendation}")
                    print(f"\n{oracle_ddl}")
                    print("-" * 80 + "\n")
            
            # Résumé final
            print(f"\n{'='*80}")
            print("RÉSUMÉ DE LA MIGRATION DES INDEX")
            print(f"{'='*80}")
            print(f"✓ Index automatiques ignorés : {len(auto_indexes)}")
            print(f"✓ Index manuels/FK convertis : {len(manual_indexes)}")
            print(f"\n💡 CONSEILS POST-MIGRATION :")
            print(f"   - Les index BITMAP sont optimaux pour colonnes à faible cardinalité (<5%)")
            print(f"   - Vérifiez la performance après création en environnement de test")
            print(f"   - Utilisez EXPLAIN PLAN pour valider l'utilisation des index")
            print(f"   - Surveillez les performances OLTP si vous utilisez des BITMAP")
            print(f"{'='*80}\n")


# Exemple d'utilisation
if __name__ == "__main__":
    conn_params = {
        'dbname': 'your_db_name',
        'user': 'your_user',
        'password': 'your_password',
        'host': 'localhost',
        'port': 5432
    }
    
    generate_indexes_ddl(conn_params)
