# -*- coding: utf-8 -*-
"""
Script: migrate_data_final.py

MIGRATION COMPLÈTE AVEC GESTION AUTOMATIQUE DES NULL
PostgreSQL → Oracle

✅ Nettoyage automatique des tables Oracle
✅ Détection des colonnes NOT NULL dans Oracle
✅ Remplacement automatique des NULL par défauts
✅ Gestion correcte des dates
✅ Aucune modification dans PostgreSQL
✅ Migration complète en une seule commande
"""

import sys
import os

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

import psycopg2
import oracledb
import json
from datetime import datetime

sys.path.insert(0, r"D:\MEMOIRE\PROJET")
try:
    from type_mapping import quote_identifier_if_needed
except:
    def quote_identifier_if_needed(name):
        reserved = ['ORDER', 'SESSION', 'USER', 'DATE', 'LEVEL', 'SIZE', 
                   'GROUP', 'COMMENT', 'ROWS', 'COLUMN', 'VALUE', 'TIME']
        if name.upper() in reserved:
            return f'"{name}"'
        return name

# ============================================================================
# CONFIGURATION
# ============================================================================

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

BATCH_SIZE = 1000
COMMIT_FREQUENCY = 10

# Valeurs par défaut pour remplacer les NULL selon le type Oracle
DEFAULT_VALUES_BY_TYPE = {
    'VARCHAR2': 'N/A',
    'CHAR': 'N/A',
    'NVARCHAR2': 'N/A',
    'NCHAR': 'N/A',
    'CLOB': 'N/A',
    'NUMBER': 0,
    'INTEGER': 0,
    'FLOAT': 0.0,
    'DATE': datetime(1900, 1, 1),
    'TIMESTAMP': datetime(1900, 1, 1),
}

# ============================================================================
# ÉTAPE 0 : NETTOYAGE DES TABLES ORACLE
# ============================================================================

def clean_oracle_tables():
    """Vide les tables Oracle"""
    print("\n" + "="*80)
    print("ÉTAPE 0 : NETTOYAGE DES TABLES ORACLE")
    print("="*80 + "\n")
    
    try:
        oracle_conn = oracledb.connect(**ORACLE_CONFIG)
        oracle_cursor = oracle_conn.cursor()
        
        oracle_cursor.execute("""
            SELECT table_name 
            FROM user_tables 
            WHERE table_name NOT LIKE 'BIN$%'
            ORDER BY table_name
        """)
        
        all_tables = [row[0] for row in oracle_cursor.fetchall()]
        
        print(f"Suppression des données de {len(all_tables)} tables...\n")
        
        # Désactiver les FK
        oracle_cursor.execute("""
            BEGIN
              FOR c IN (SELECT constraint_name, table_name FROM user_constraints 
                       WHERE constraint_type = 'R' AND status = 'ENABLED') LOOP
                EXECUTE IMMEDIATE 'ALTER TABLE "' || c.table_name || 
                                 '" DISABLE CONSTRAINT "' || c.constraint_name || '"';
              END LOOP;
            END;
        """)
        
        oracle_conn.commit()
        
        # Supprimer les données
        for table in all_tables:
            try:
                oracle_cursor.execute(f'DELETE FROM "{table}"')
                count = oracle_cursor.rowcount
                if count > 0:
                    print(f"  ✅ {table:40} : {count:>10,} supprimées")
                oracle_conn.commit()
            except:
                oracle_conn.rollback()
        
        # Réactiver les FK
        oracle_cursor.execute("""
            BEGIN
              FOR c IN (SELECT constraint_name, table_name FROM user_constraints 
                       WHERE constraint_type = 'R' AND status = 'DISABLED') LOOP
                EXECUTE IMMEDIATE 'ALTER TABLE "' || c.table_name || 
                                 '" ENABLE CONSTRAINT "' || c.constraint_name || '"';
              END LOOP;
            END;
        """)
        
        oracle_conn.commit()
        oracle_cursor.close()
        oracle_conn.close()
        
        print("\n✅ Nettoyage terminé\n")
        return True
        
    except Exception as e:
        print(f"❌ Erreur : {e}\n")
        return False

# ============================================================================
# ÉTAPE 1 : DÉCOUVERTE MAPPING + CONTRAINTES NOT NULL
# ============================================================================

def discover_mapping_and_constraints():
    """
    Découvre :
    - Le mapping tables/colonnes
    - Quelles colonnes sont NOT NULL dans Oracle
    """
    print("\n" + "="*80)
    print("ÉTAPE 1 : DÉCOUVERTE DU MAPPING & CONTRAINTES")
    print("="*80 + "\n")
    
    try:
        oracle_conn = oracledb.connect(**ORACLE_CONFIG)
        oracle_cursor = oracle_conn.cursor()
        
        pg_conn = psycopg2.connect(**PG_CONFIG)
        pg_cursor = pg_conn.cursor()
        
        # Récupérer les tables
        oracle_cursor.execute("""
            SELECT table_name 
            FROM user_tables 
            WHERE table_name NOT LIKE 'BIN$%'
        """)
        
        oracle_tables = {row[0] for row in oracle_cursor.fetchall()}
        
        pg_cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
        """)
        
        pg_tables = [row[0] for row in pg_cursor.fetchall()]
        
        table_mapping = {}
        column_mapping = {}
        column_types_mapping = {}
        not_null_constraints = {}  # {table: {column: True/False, ...}, ...}
        
        print(f"Analyse de {len(pg_tables)} tables...\n")
        
        for pg_table in pg_tables:
            # Trouver la table Oracle correspondante
            oracle_table = None
            for ora_table in oracle_tables:
                if ora_table.lower() == pg_table.lower():
                    oracle_table = ora_table
                    break
            
            if oracle_table is None:
                continue
            
            table_mapping[pg_table] = oracle_table
            
            # Récupérer les colonnes PostgreSQL
            pg_cursor.execute("""
                SELECT column_name, data_type, udt_name, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (pg_table,))
            
            pg_columns_info = pg_cursor.fetchall()
            pg_columns = {col[0]: col for col in pg_columns_info}
            
            # Récupérer les colonnes Oracle + NOT NULL
            oracle_cursor.execute(f"""
                SELECT column_name, data_type, nullable
                FROM user_tab_columns
                WHERE table_name = '{oracle_table}'
                ORDER BY column_id
            """)
            
            oracle_columns = {row[0]: row for row in oracle_cursor.fetchall()}
            
            col_map = {}
            col_types = {}
            col_nullable = {}
            
            for pg_col, pg_info in pg_columns.items():
                pg_data_type = pg_info[1]
                pg_udt_type = pg_info[2]
                
                # Trouver la colonne Oracle
                oracle_col = None
                for ora_col in oracle_columns.keys():
                    if ora_col.lower() == pg_col.lower():
                        oracle_col = ora_col
                        break
                
                if oracle_col is None:
                    oracle_conn.close()
                    pg_conn.close()
                    return None
                
                col_map[pg_col] = oracle_col
                col_types[pg_col] = {
                    'pg_type': pg_data_type,
                    'pg_udt': pg_udt_type,
                    'oracle_type': oracle_columns[oracle_col][1],
                    'oracle_nullable': oracle_columns[oracle_col][2]  # 'Y' ou 'N'
                }
                
                # Enregistrer si la colonne est NOT NULL dans Oracle
                is_not_null = oracle_columns[oracle_col][2] == 'N'
                col_nullable[pg_col] = is_not_null
            
            column_mapping[pg_table] = col_map
            column_types_mapping[pg_table] = col_types
            not_null_constraints[pg_table] = col_nullable
        
        oracle_conn.close()
        pg_conn.close()
        
        print(f"✅ Mapping créé pour {len(table_mapping)} tables\n")
        
        return {
            'tables': table_mapping,
            'columns': column_mapping,
            'column_types': column_types_mapping,
            'not_null': not_null_constraints
        }
        
    except Exception as e:
        print(f"❌ Erreur : {e}\n")
        import traceback
        traceback.print_exc()
        return None

# ============================================================================
# ÉTAPE 2 : DÉTECTION ORDRE TABLES
# ============================================================================

def get_tables_order_auto(pg_table_names):
    """Détecte l'ordre des tables"""
    print("\n" + "="*80)
    print("ÉTAPE 2 : ORDRE DE MIGRATION")
    print("="*80 + "\n")
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        all_tables = pg_table_names
        
        cursor.execute("""
            SELECT tc.table_name AS from_table, ccu.table_name AS to_table
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'public'
        """)
        
        dependencies = cursor.fetchall()
        
        dep_graph = {table: [] for table in all_tables}
        
        for from_table, to_table in dependencies:
            if from_table in dep_graph and to_table in all_tables:
                dep_graph[from_table].append(to_table)
        
        ordered_tables = []
        visited = set()
        
        def visit(table):
            if table in visited:
                return
            visited.add(table)
            for dep in dep_graph.get(table, []):
                visit(dep)
            ordered_tables.append(table)
        
        for table in all_tables:
            visit(table)
        
        print(f"✅ Ordre calculé ({len(ordered_tables)} tables)\n")
        
        cursor.close()
        conn.close()
        
        return ordered_tables
        
    except Exception as e:
        print(f"❌ Erreur : {e}\n")
        return []

# ============================================================================
# ÉTAPE 3 : CONVERSION DONNÉES AVEC GESTION DES NULL
# ============================================================================

def get_default_value_for_type(oracle_type):
    """Retourne une valeur par défaut basée sur le type Oracle"""
    oracle_type_upper = oracle_type.upper()
    
    for key, value in DEFAULT_VALUES_BY_TYPE.items():
        if key in oracle_type_upper:
            return value
    
    # Par défaut, retourner une chaîne vide pour les types inconnus
    return 'N/A'

def format_timestamp_for_oracle(value):
    """Convertit timestamp PostgreSQL → Oracle"""
    if value is None:
        return None
    
    if hasattr(value, 'isoformat'):
        return value
    
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            return dt
        except:
            return value
    
    return value

def convert_value_for_oracle(value, col_type_info, is_not_null):
    """
    Convertit valeur PostgreSQL → Oracle
    Si NULL et NOT NULL required → utilise une valeur par défaut
    """
    # Si la valeur est NULL et la colonne est NOT NULL
    if value is None and is_not_null:
        oracle_type = col_type_info.get('oracle_type', '')
        default_value = get_default_value_for_type(oracle_type)
        return default_value
    
    if value is None:
        return None
    
    pg_type = col_type_info.get('pg_type', '')
    pg_udt = col_type_info.get('pg_udt', '')
    
    # TIMESTAMPS
    if 'timestamp' in pg_type.lower():
        return format_timestamp_for_oracle(value)
    
    # BOOLEAN
    if pg_type == 'boolean':
        return 1 if value else 0
    
    # JSON
    if pg_type in ('json', 'jsonb') or pg_udt in ('json', 'jsonb'):
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value)
    
    # UUID
    if pg_type == 'uuid' or pg_udt == 'uuid':
        return str(value)
    
    # USER-DEFINED
    if pg_type == 'USER-DEFINED':
        return str(value)
    
    # ARRAYS
    if isinstance(value, list):
        return json.dumps(value)
    
    return value

# ============================================================================
# ÉTAPE 4 : MIGRATION DONNÉES
# ============================================================================

def migrate_table(pg_table_name, mapping_info, pg_conn, oracle_conn):
    """Migre une table avec gestion des NULL"""
    try:
        pg_cursor = pg_conn.cursor()
        
        oracle_table_name = mapping_info['tables'][pg_table_name]
        column_map = mapping_info['columns'][pg_table_name]
        column_types = mapping_info['column_types'][pg_table_name]
        not_null = mapping_info['not_null'][pg_table_name]
        
        # Compter les lignes
        pg_cursor.execute(f'SELECT COUNT(*) FROM "{pg_table_name}"')
        row_count = pg_cursor.fetchone()[0]
        
        print(f"[{pg_table_name:40}] ", end="", flush=True)
        
        if row_count == 0:
            print("(vide)")
            pg_cursor.close()
            return True, 0
        
        # Récupérer les colonnes
        pg_cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'public'
            ORDER BY ordinal_position
        """, (pg_table_name,))
        
        pg_column_names = [row[0] for row in pg_cursor.fetchall()]
        
        # Construire les requêtes
        col_list_pg = ', '.join([f'"{col}"' for col in pg_column_names])
        col_list_ora = ', '.join([f'"{column_map[col]}"' for col in pg_column_names])
        
        select_query = f'SELECT {col_list_pg} FROM "{pg_table_name}"'
        placeholders = ', '.join([f':{i+1}' for i in range(len(pg_column_names))])
        insert_query = f'INSERT INTO "{oracle_table_name}" ({col_list_ora}) VALUES ({placeholders})'
        
        # Récupérer les données
        pg_cursor_batch = pg_conn.cursor(name=f'batch_{pg_table_name}')
        pg_cursor_batch.itersize = BATCH_SIZE
        pg_cursor_batch.execute(select_query)
        
        oracle_cursor = oracle_conn.cursor()
        
        batch = []
        total_inserted = 0
        commit_count = 0
        
        for row in pg_cursor_batch:
            converted_row = []
            
            for i, col_name in enumerate(pg_column_names):
                col_is_not_null = not_null.get(col_name, False)
                converted_value = convert_value_for_oracle(
                    row[i], 
                    column_types[col_name],
                    col_is_not_null
                )
                converted_row.append(converted_value)
            
            batch.append(converted_row)
            
            if len(batch) >= BATCH_SIZE:
                oracle_cursor.executemany(insert_query, batch)
                total_inserted += len(batch)
                
                commit_count += 1
                if commit_count % COMMIT_FREQUENCY == 0:
                    oracle_conn.commit()
                
                batch = []
        
        # Dernières lignes
        if batch:
            oracle_cursor.executemany(insert_query, batch)
            total_inserted += len(batch)
        
        oracle_conn.commit()
        
        print(f"✅ {total_inserted:,} lignes migrées")
        
        pg_cursor_batch.close()
        pg_cursor.close()
        oracle_cursor.close()
        
        return True, total_inserted
        
    except Exception as e:
        print(f"❌ ERREUR : {str(e)[:70]}")
        return False, 0

def migrate_all_tables(mapping_info, table_order):
    """Migre toutes les tables"""
    print("\n" + "="*80)
    print("ÉTAPE 4 : MIGRATION DES DONNÉES")
    print("="*80 + "\n")
    
    try:
        pg_conn = psycopg2.connect(**PG_CONFIG)
        oracle_conn = oracledb.connect(**ORACLE_CONFIG)
        
        print(f"Migration de {len(table_order)} tables (avec gestion des NULL)\n")
        print("-"*80)
        
        start_time = datetime.now()
        
        total_tables_success = 0
        total_rows = 0
        errors = []
        
        for pg_table in table_order:
            if pg_table not in mapping_info['tables']:
                continue
            
            success, rows = migrate_table(pg_table, mapping_info, pg_conn, oracle_conn)
            
            if success:
                total_tables_success += 1
                total_rows += rows
            else:
                errors.append(pg_table)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        print("-"*80)
        
        pg_conn.close()
        oracle_conn.close()
        
        return total_tables_success, total_rows, duration, errors
        
    except Exception as e:
        print(f"\n❌ ERREUR : {e}\n")
        return 0, 0, 0, []

# ============================================================================
# ÉTAPE 5 : RAPPORT FINAL
# ============================================================================

def print_final_report(tables_migrated, total_tables, total_rows, duration, errors):
    """Rapport final"""
    print("\n" + "="*80)
    print("ÉTAPE 5 : RAPPORT FINAL")
    print("="*80)
    print()
    
    print(f"✅ Tables réussies : {tables_migrated}/{total_tables}")
    print(f"✅ Total lignes migrées : {total_rows:,}")
    print(f"✅ Durée totale : {duration:.2f} secondes")
    
    if total_rows > 0 and duration > 0:
        speed = int(total_rows / duration)
        print(f"✅ Vitesse moyenne : {speed:,} lignes/seconde")
    
    if errors:
        print(f"\n⚠️ Tables en erreur ({len(errors)}):")
        for table in errors:
            print(f"  - {table}")
    else:
        print("\n🎉 MIGRATION RÉUSSIE - TOUTES LES DONNÉES MIGRÉES SANS ERREUR !")
    
    print("\n" + "="*80)
    print("MIGRATION TERMINÉE")
    print("="*80 + "\n")

# ============================================================================
# FONCTION PRINCIPALE
# ============================================================================

def main():
    print("\n" + "="*80)
    print("MIGRATION COMPLÈTE AVEC GESTION AUTOMATIQUE DES NULL")
    print("PostgreSQL → Oracle")
    print("="*80)
    print(f"\nDate : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Source : {PG_CONFIG['database']}@{PG_CONFIG['host']}")
    print(f"Cible : {ORACLE_CONFIG['user']}@{ORACLE_CONFIG['dsn']}")
    print(f"\nFonctionnalités :")
    print(f"  • Nettoyage automatique des tables Oracle")
    print(f"  • Détection des colonnes NOT NULL")
    print(f"  • Remplacement automatique des NULL par défauts")
    print(f"  • Aucune modification dans PostgreSQL")
    
    # ÉTAPE 0 : Nettoyer
    response = input("\n▶ Étape 0 : Vider les tables Oracle ? (o/n) : ").strip().lower()
    
    if response == 'o':
        if not clean_oracle_tables():
            return
    
    # ÉTAPE 1 : Mapping
    mapping_info = discover_mapping_and_constraints()
    
    if mapping_info is None:
        print("\n❌ Impossible de créer le mapping.\n")
        return
    
    pg_table_names = list(mapping_info['tables'].keys())
    
    # ÉTAPE 2 : Ordre
    table_order = get_tables_order_auto(pg_table_names)
    
    if not table_order:
        print("\n❌ Impossible de calculer l'ordre.\n")
        return
    
    print(f"\nOrdre de migration ({len(table_order)} tables) :")
    for i, pg_table in enumerate(table_order[:3], 1):
        print(f"  {i}. {pg_table}")
    if len(table_order) > 3:
        print(f"  ... et {len(table_order)-3} autres tables")
    
    print("\n" + "-"*80)
    
    response = input("\n▶ Étape 4 : Commencer la migration ? (o/n) : ").strip().lower()
    
    if response != 'o':
        print("\n⚠️ Migration annulée.\n")
        return
    
    # ÉTAPE 3-4 : Migrer
    tables_migrated, total_rows, duration, errors = migrate_all_tables(mapping_info, table_order)
    
    # ÉTAPE 5 : Rapport
    print_final_report(tables_migrated, len(table_order), total_rows, duration, errors)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Migration interrompue (Ctrl+C)\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERREUR FATALE : {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
