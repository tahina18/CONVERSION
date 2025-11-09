"""
Module collection_views.py
--------------------------
Ce module contient toutes les fonctions pour la collecte, l'analyse,
la manipulation et la conversion des vues PostgreSQL vers Oracle.
"""

import psycopg2
import re


def collect_postgresql_views(cursor):
    """
    Récupère toutes les vues PostgreSQL avec leurs détails et affiche les résultats.
    
    :param cursor: curseur psycopg2 connecté à PostgreSQL
    :return: liste des vues récupérées
    """
    query = """
    SELECT
        schemaname AS schema_name,
        viewname AS view_name,
        definition AS view_definition
    FROM pg_views
    WHERE schemaname = 'public'
    ORDER BY viewname;
    """
    
    cursor.execute(query)
    views = cursor.fetchall()
    
    # Affichage des résultats
    print(f"\n{'='*80}")
    print(f"COLLECTE DES VUES POSTGRESQL")
    print(f"{'='*80}\n")
    
    if not views:
        print("Aucune vue trouvée dans le schéma 'public'.\n")
        return views
    
    print(f"Nombre total de vues trouvées : {len(views)}\n")
    
    for idx, view in enumerate(views, 1):
        schema, view_name, definition = view
        
        print(f"--- VUE #{idx} ---")
        print(f"Nom de la vue       : {view_name}")
        print(f"Schéma              : {schema}")
        print(f"Définition          :")
        print("-" * 40)
        print(definition[:200] + "..." if len(definition) > 200 else definition)
        print("-" * 40)
        print()
    
    print(f"{'='*80}\n")
    
    return views


def collect_postgresql_materialized_views(cursor):
    """
    Récupère toutes les vues matérialisées PostgreSQL.
    
    :param cursor: curseur psycopg2 connecté à PostgreSQL
    :return: liste des vues matérialisées
    """
    query = """
    SELECT
        schemaname AS schema_name,
        matviewname AS view_name,
        definition AS view_definition
    FROM pg_matviews
    WHERE schemaname = 'public'
    ORDER BY matviewname;
    """
    
    cursor.execute(query)
    mat_views = cursor.fetchall()
    
    # Affichage des résultats
    print(f"\n{'='*80}")
    print(f"COLLECTE DES VUES MATÉRIALISÉES POSTGRESQL")
    print(f"{'='*80}\n")
    
    if not mat_views:
        print("Aucune vue matérialisée trouvée dans le schéma 'public'.\n")
        return mat_views
    
    print(f"Nombre total de vues matérialisées trouvées : {len(mat_views)}\n")
    
    for idx, view in enumerate(mat_views, 1):
        schema, view_name, definition = view
        
        print(f"--- VUE MATÉRIALISÉE #{idx} ---")
        print(f"Nom                 : {view_name}")
        print(f"Schéma              : {schema}")
        print(f"Définition          :")
        print("-" * 40)
        print(definition[:200] + "..." if len(definition) > 200 else definition)
        print("-" * 40)
        print()
    
    print(f"{'='*80}\n")
    
    return mat_views


def convert_sql_postgresql_to_oracle(sql_definition):
    """
    Convertit une requête SQL PostgreSQL vers Oracle.
    Gère les principales différences de syntaxe.
    
    :param sql_definition: définition SQL PostgreSQL
    :return: définition SQL Oracle
    """
    oracle_sql = sql_definition
    
    # Conversion des types de données dans les CAST
    oracle_sql = re.sub(r'::(\w+)', r'', oracle_sql)  # Supprimer les cast :: PostgreSQL
    
    # Conversion des fonctions de chaînes
    oracle_sql = oracle_sql.replace('||', '||')  # Déjà compatible
    oracle_sql = re.sub(r'\bCONCAT\s*\(', 'CONCAT(', oracle_sql, flags=re.IGNORECASE)
    
    # Conversion des fonctions de date
    oracle_sql = oracle_sql.replace('CURRENT_TIMESTAMP', 'SYSTIMESTAMP')
    oracle_sql = oracle_sql.replace('NOW()', 'SYSDATE')
    oracle_sql = oracle_sql.replace('CURRENT_DATE', 'TRUNC(SYSDATE)')
    
    # Conversion LIMIT vers ROWNUM ou FETCH FIRST (Oracle 12c+)
    limit_match = re.search(r'LIMIT\s+(\d+)', oracle_sql, re.IGNORECASE)
    if limit_match:
        limit_value = limit_match.group(1)
        oracle_sql = re.sub(r'LIMIT\s+\d+', f'FETCH FIRST {limit_value} ROWS ONLY', oracle_sql, flags=re.IGNORECASE)
    
    # Conversion OFFSET
    offset_match = re.search(r'OFFSET\s+(\d+)', oracle_sql, re.IGNORECASE)
    if offset_match:
        offset_value = offset_match.group(1)
        oracle_sql = re.sub(r'OFFSET\s+\d+', f'OFFSET {offset_value} ROWS', oracle_sql, flags=re.IGNORECASE)
    
    # Conversion des booléens
    oracle_sql = re.sub(r'\bTRUE\b', '1', oracle_sql, flags=re.IGNORECASE)
    oracle_sql = re.sub(r'\bFALSE\b', '0', oracle_sql, flags=re.IGNORECASE)
    
    # Conversion ILIKE vers UPPER() LIKE
    oracle_sql = re.sub(
        r'(\w+)\s+ILIKE\s+([^\s]+)',
        r'UPPER(\1) LIKE UPPER(\2)',
        oracle_sql,
        flags=re.IGNORECASE
    )
    
    # Conversion des expressions régulières
    oracle_sql = re.sub(r'~\s+', 'REGEXP_LIKE(', oracle_sql)
    oracle_sql = re.sub(r'~\*\s+', 'REGEXP_LIKE(', oracle_sql)
    
    return oracle_sql


def generate_oracle_view_ddl(view_name, pg_definition):
    """
    Génère le DDL Oracle pour une vue PostgreSQL.
    
    :param view_name: nom de la vue
    :param pg_definition: définition SQL PostgreSQL de la vue
    :return: DDL Oracle de la vue
    """
    # Convertir la définition SQL
    oracle_definition = convert_sql_postgresql_to_oracle(pg_definition)
    
    # Nettoyer la définition (supprimer les espaces en début/fin)
    oracle_definition = oracle_definition.strip()
    
    # S'assurer que la définition ne se termine pas par un point-virgule
    if oracle_definition.endswith(';'):
        oracle_definition = oracle_definition[:-1]
    
    # Générer le DDL
    ddl = f"CREATE OR REPLACE VIEW {view_name} AS\n"
    ddl += oracle_definition
    ddl += ";\n"
    
    return ddl


def generate_oracle_materialized_view_ddl(view_name, pg_definition):
    """
    Génère le DDL Oracle pour une vue matérialisée PostgreSQL.
    
    :param view_name: nom de la vue matérialisée
    :param pg_definition: définition SQL PostgreSQL
    :return: DDL Oracle de la vue matérialisée
    """
    # Convertir la définition SQL
    oracle_definition = convert_sql_postgresql_to_oracle(pg_definition)
    
    # Nettoyer la définition
    oracle_definition = oracle_definition.strip()
    if oracle_definition.endswith(';'):
        oracle_definition = oracle_definition[:-1]
    
    # Générer le DDL pour vue matérialisée Oracle
    ddl = f"CREATE MATERIALIZED VIEW {view_name}\n"
    ddl += f"BUILD IMMEDIATE\n"
    ddl += f"REFRESH COMPLETE ON DEMAND\n"
    ddl += f"AS\n"
    ddl += oracle_definition
    ddl += ";\n"
    
    return ddl


def generate_views_ddl(connection_params):
    """
    Fonction principale pour générer les DDL Oracle des vues.
    Collecte, analyse et convertit toutes les vues PostgreSQL.
    
    :param connection_params: paramètres de connexion PostgreSQL
    """
    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cursor:
            # Étape 1 : Collecter les vues simples
            views = collect_postgresql_views(cursor)
            
            # Étape 2 : Collecter les vues matérialisées
            mat_views = collect_postgresql_materialized_views(cursor)
            
            # Étape 3 : Générer les DDL pour les vues simples
            if views:
                print(f"\n{'='*80}")
                print("CONVERSION DES VUES EN ORACLE")
                print(f"{'='*80}\n")
                
                for schema, view_name, definition in views:
                    oracle_ddl = generate_oracle_view_ddl(view_name, definition)
                    
                    print(f"-- Vue PostgreSQL: {view_name}")
                    print(f"-- Schéma: {schema}")
                    print(f"\n{oracle_ddl}")
                    print("-" * 80 + "\n")
            
            # Étape 4 : Générer les DDL pour les vues matérialisées
            if mat_views:
                print(f"\n{'='*80}")
                print("CONVERSION DES VUES MATÉRIALISÉES EN ORACLE")
                print(f"{'='*80}\n")
                
                for schema, view_name, definition in mat_views:
                    oracle_ddl = generate_oracle_materialized_view_ddl(view_name, definition)
                    
                    print(f"-- Vue matérialisée PostgreSQL: {view_name}")
                    print(f"-- Schéma: {schema}")
                    print(f"\n{oracle_ddl}")
                    print("-" * 80 + "\n")
            
            # Résumé final
            print(f"\n{'='*80}")
            print("RÉSUMÉ DE LA MIGRATION DES VUES")
            print(f"{'='*80}")
            print(f"✓ Vues simples converties : {len(views)}")
            print(f"✓ Vues matérialisées converties : {len(mat_views)}")
            print(f"\n⚠ ATTENTION :")
            print(f"   - Vérifiez manuellement les conversions SQL complexes")
            print(f"   - Testez toutes les vues en Oracle avant la mise en production")
            print(f"   - Les vues matérialisées Oracle nécessitent une stratégie de REFRESH")
            print(f"{'='*80}\n")


# Exemple d'utilisation
if __name__ == "__main__":
    conn_params = {
        'dbname': 'AURA',
        'user': 'postgres',
        'password': 'admin',
        'host': 'localhost',
        'port': 5432
    }
    
    generate_views_ddl(conn_params)
