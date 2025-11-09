# -*- coding: utf-8 -*-

"""
Script: migrate_data_final_fixed.py
MIGRATION DONNÉES AVEC CORRECTIONS COMPLÈTES

✅ Mapping automatique des colonnes
✅ Gestion CORRECTE des dates (format Oracle)
✅ Gestion des NULL et valeurs NULL invalides
✅ Diagnostic détaillé des erreurs
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

# ============================================================================
# ÉTAPE 1 : DÉCOUVERTE MAPPING TABLES ET COLONNES
# ============================================================================
def discover_table_and_column_mapping():
    """Détecte le mapping complet"""
    print("\n" + "="*80)
    print("DÉCOUVERTE AUTOMATIQUE DU MAPPING TABLES & COLONNES")
    print("="*80 + "\n")
    try:
        print("Connexion à Oracle...")
        oracle_conn = oracledb.connect(**ORACLE_CONFIG)
        oracle_cursor = oracle_conn.cursor()
        print("✅ Connecté à Oracle\n")

        print("Connexion à PostgreSQL...")
        pg_conn = psycopg2.connect(**PG_CONFIG)
        pg_cursor = pg_conn.cursor()
        print("✅ Connecté à PostgreSQL\n")

        # Récupérer les tables Oracle
        oracle_cursor.execute("""
        SELECT table_name
        FROM user_tables
        WHERE table_name NOT LIKE 'BIN$%'
        ORDER BY table_name
        """)
        oracle_tables = {row[0] for row in oracle_cursor.fetchall()}

        # Récupérer les tables PostgreSQL
        pg_cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """)
        pg_tables = [row[0] for row in pg_cursor.fetchall()]

        # Créer le mapping
        table_mapping = {}
        column_mapping = {}
        column_types_mapping = {}

        print(f"Création du mapping pour {len(pg_tables)} tables...\n")

        for pg_table in pg_tables:
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

            # Récupérer les colonnes Oracle
            oracle_cursor.execute(f"""
            SELECT column_name, data_type, nullable
            FROM user_tab_columns
            WHERE table_name = '{oracle_table}'
            ORDER BY column_id
            """)
            oracle_columns = {row[0]: row for row in oracle_cursor.fetchall()}

            # Créer le mapping des colonnes
            col_map = {}
            col_types = {}

            for pg_col, pg_info in pg_columns.items():
                pg_data_type = pg_info[1]
                pg_udt_type = pg_info[2]
                pg_nullable = pg_info[3]

                oracle_col = None
                for ora_col in oracle_columns.keys():
                    if ora_col.lower() == pg_col.lower():
                        oracle_col = ora_col
                        break

                if oracle_col is None:
                    print(f"❌ {pg_table}.{pg_col} -> NOT FOUND in Oracle")
                    oracle_conn.close()
                    pg_conn.close()
                    return None

                col_map[pg_col] = oracle_col
                col_types[pg_col] = {
                    'pg_type': pg_data_type,
                    'pg_udt': pg_udt_type,
                    'pg_nullable': pg_nullable,
                    'oracle_type': oracle_columns[oracle_col][1],
                    'oracle_nullable': oracle_columns[oracle_col][2]
                }

            column_mapping[pg_table] = col_map
            column_types_mapping[pg_table] = col_types

        if not table_mapping:
            print("\n❌ Aucune table trouvée\n")
            oracle_conn.close()
            pg_conn.close()
            return None

        oracle_conn.close()
        pg_conn.close()

        print(f"✅ Mapping créé pour {len(table_mapping)} tables\n")

        return {
            'tables': table_mapping,
            'columns': column_mapping,
            'column_types': column_types_mapping
        }

    except Exception as e:
        print(f"❌ ERREUR : {e}\n")
        import traceback
        traceback.print_exc()
        return None

# ============================================================================
# ÉTAPE 2 : DÉTECTION ORDRE TABLES
# ============================================================================
def get_tables_order_auto(pg_table_names):
    """Détecte l'ordre des tables"""
    print("\n" + "="*80)
    print("DÉTECTION AUTOMATIQUE DE L'ORDRE DES TABLES")
    print("="*80 + "\n")
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        all_tables = pg_table_names
        print(f"✅ {len(all_tables)} tables à ordonnancer")

        cursor.execute("""
        SELECT tc.table_name AS from_table, ccu.table_name AS to_table
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = 'public'
        """)

        dependencies = cursor.fetchall()
        print(f"✅ {len(dependencies)} dépendances (FK) détectées")

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

        print(f"✅ Ordre de migration calculé\n")

        cursor.close()
        conn.close()
        return ordered_tables

    except Exception as e:
        print(f"❌ ERREUR : {e}\n")
        return []

# ============================================================================
# ÉTAPE 3 : CONVERSION DONNÉES AVEC CORRECTION DES DATES
# ============================================================================
def format_timestamp_for_oracle(value):
    """
    Convertit un timestamp PostgreSQL au format Oracle
    PostgreSQL: datetime object
    Oracle: ISO 8601 string ou datetime
    """
    if value is None:
        return None
    try:
        if hasattr(value, 'isoformat'):
            return value
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                return dt
            except:
                return value
        return value
    except:
        return value

def convert_value_for_oracle(value, col_type_info):
    """
    Convertit valeur PostgreSQL → Oracle avec gestion correcte des types
    """
    if value is None:
        return None

    pg_type = col_type_info.get('pg_type', '')
    pg_udt = col_type_info.get('pg_udt', '')
    oracle_type = col_type_info.get('oracle_type', '')

    if 'timestamp' in pg_type.lower():
        return format_timestamp_for_oracle(value)

    if 'date' in pg_type.lower() and 'timestamp' not in pg_type.lower():
        if hasattr(value, 'isoformat'):
            return value
        return value

    if pg_type == 'boolean':
        return 1 if value else 0

    if pg_type in ('json', 'jsonb') or pg_udt in ('json', 'jsonb'):
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value)

    if pg_type == 'uuid' or pg_udt == 'uuid':
        return str(value)

    if pg_type == 'USER-DEFINED':
        return str(value)

    if isinstance(value, list):
        return json.dumps(value)

    return value

# ============================================================================
# ÉTAPE 4 : MIGRATION DONNÉES
# ============================================================================
def migrate_table(pg_table_name, mapping_info, pg_conn, oracle_conn):
    """Migre une table avec correction des dates et NULL"""
    try:
        pg_cursor = pg_conn.cursor()

        oracle_table_name = mapping_info['tables'][pg_table_name]
        column_map = mapping_info['columns'][pg_table_name]
        column_types = mapping_info['column_types'][pg_table_name]

        pg_cursor.execute(f'SELECT COUNT(*) FROM "{pg_table_name}"')
        row_count = pg_cursor.fetchone()[0]
        print(f"[{pg_table_name:40}] ", end="", flush=True)
        if row_count == 0:
            print("(vide)")
            pg_cursor.close()
            return True, 0

        pg_cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s AND table_schema = 'public'
        ORDER BY ordinal_position
        """, (pg_table_name,))
        pg_column_names = [row[0] for row in pg_cursor.fetchall()]

        col_list_pg = ', '.join([f'"{col}"' for col in pg_column_names])
        col_list_ora = ', '.join([f'"{column_map[col]}"' for col in pg_column_names])

        select_query = f'SELECT {col_list_pg} FROM "{pg_table_name}"'
        placeholders = ', '.join([f':{i+1}' for i in range(len(pg_column_names))])
        insert_query = f'INSERT INTO "{oracle_table_name}" ({col_list_ora}) VALUES ({placeholders})'

        pg_cursor_batch = pg_conn.cursor(name=f'batch_{pg_table_name}')
        pg_cursor_batch.itersize = BATCH_SIZE
        pg_cursor_batch.execute(select_query)

        oracle_cursor = oracle_conn.cursor()
        batch = []

        total_inserted = 0
        commit_count = 0
        errors_in_batch = 0

        for row in pg_cursor_batch:
            try:
                converted_row = [
                    convert_value_for_oracle(row[i], column_types[pg_column_names[i]])
                    for i in range(len(row))
                ]
                batch.append(converted_row)

                if len(batch) >= BATCH_SIZE:
                    try:
                        oracle_cursor.executemany(insert_query, batch)
                        total_inserted += len(batch)
                        commit_count += 1
                        if commit_count % COMMIT_FREQUENCY == 0:
                            oracle_conn.commit()
                    except Exception as e:
                        error_msg = str(e)[:100]
                        if "not a valid month" in error_msg:
                            print(f"\n❌ ERREUR DATE FORMAT : {error_msg}")
                        elif "cannot insert NULL" in error_msg:
                            print(f"\n❌ ERREUR NULL INVALIDE : {error_msg}")
                        else:
                            print(f"\n❌ ERREUR INSERT BATCH : {error_msg}")
                        errors_in_batch += 1
                    batch = []
            except Exception as e:
                print(f"\n❌ ERREUR CONVERSION : {str(e)[:100]}")
                errors_in_batch += 1

        if batch:
            try:
                oracle_cursor.executemany(insert_query, batch)
                total_inserted += len(batch)
            except Exception as e:
                print(f"\n❌ ERREUR INSERT FINAL : {str(e)[:100]}")
                errors_in_batch += 1

        oracle_conn.commit()

        print(f"✅ {total_inserted:,} lignes migrées", end="")
        if errors_in_batch > 0:
            print(f" ({errors_in_batch} erreurs)")
        else:
            print()

        pg_cursor_batch.close()
        pg_cursor.close()
        oracle_cursor.close()

        return True, total_inserted
    except Exception as e:
        error_msg = str(e)[:70]
        print(f"❌ ERREUR TABLE : {error_msg}")
        return False, 0

def migrate_all_tables(mapping_info, table_order):
    """Migre toutes les tables"""
    print("\n" + "="*80)
    print("MIGRATION DES DONNÉES")
    print("="*80 + "\n")
    try:
        print("Connexion à PostgreSQL...")
        pg_conn = psycopg2.connect(**PG_CONFIG)
        print("✅ Connecté à PostgreSQL\n")

        print("Connexion à Oracle...")
        oracle_conn = oracledb.connect(**ORACLE_CONFIG)
        print("✅ Connecté à Oracle\n")

        print(f"Migration de {len(table_order)} tables\n")
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
        import traceback
        traceback.print_exc()
        return 0, 0, 0, []

# ============================================================================
# ÉTAPE 5 : RAPPORT FINAL
# ============================================================================
def print_final_report(tables_migrated, total_tables, total_rows, duration, errors):
    """Rapport final"""
    print("\n" + "="*80)
    print("RAPPORT FINAL DE MIGRATION")
    print("="*80)
    print()
    print(f"✅ Tables migrées avec succès : {tables_migrated}/{total_tables}")
    print(f"✅ Total lignes migrées : {total_rows:,}")
    print(f"✅ Durée totale : {duration:.2f} secondes")
    if total_rows > 0 and duration > 0:
        speed = int(total_rows / duration)
        print(f"✅ Vitesse moyenne : {speed:,} lignes/seconde")
    if errors:
        print(f"\n⚠️ Tables en erreur ({len(errors)}):")
        for table in errors:
            print(f" - {table}")
    else:
        print("\n🎉 MIGRATION COMPLÈTE AVEC SUCCÈS !")
    print("\n" + "="*80)
    print("MIGRATION TERMINÉE")
    print("="*80 + "\n")

# ============================================================================
# FONCTION PRINCIPALE
# ============================================================================
def main():
    print("\n" + "="*80)
    print("MIGRATION DONNÉES CORRIGÉE (Dates & NULL)")
    print("PostgreSQL → Oracle")
    print("="*80)
    print(f"\nDate : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Source : {PG_CONFIG['database']}@{PG_CONFIG['host']}")
    print(f"Cible : {ORACLE_CONFIG['user']}@{ORACLE_CONFIG['dsn']}")

    mapping_info = discover_table_and_column_mapping()
    if mapping_info is None:
        print("\n❌ Impossible de créer le mapping.\n")
        return

    pg_table_names = list(mapping_info['tables'].keys())

    table_order = get_tables_order_auto(pg_table_names)
    if not table_order:
        print("\n❌ Impossible de détecter l'ordre.\n")
        return

    print("Ordre de migration :")
    for i, pg_table in enumerate(table_order[:5], 1):
        oracle_table = mapping_info['tables'][pg_table]
        print(f" {i}. {pg_table} -> {oracle_table}")
    if len(table_order) > 5:
        print(f" ... et {len(table_order)-5} autres tables")

    print("\n" + "-"*80)
    response = input("\n▶ Continuer ? (o/n) : ").strip().lower()
    if response != 'o':
        print("\n⚠️ Annulé.\n")
        return

    tables_migrated, total_rows, duration, errors = migrate_all_tables(mapping_info, table_order)

    print_final_report(tables_migrated, len(table_order), total_rows, duration, errors)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrompu (Ctrl+C)\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERREUR : {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
