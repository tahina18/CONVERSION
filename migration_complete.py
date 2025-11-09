# -*- coding: utf-8 -*-
"""
Script: migration_complete_v3.py

SOLUTION COMPLÈTE & AUTOMATISÉE
Exécute toutes les étapes de migration en une seule commande

Étapes:
1. ✅ Audit des données PostgreSQL
2. ✅ Génération du DDL Oracle
3. ✅ Exécution du DDL dans Oracle (création des tables)
4. ✅ Désactivation des contraintes FK
5. ✅ Migration des données
6. ✅ Réactivation des contraintes
7. ✅ Rapport final
"""

import sys
import os
from datetime import datetime
import subprocess

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

import psycopg2
import oracledb

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = r"D:\MEMOIRE\PROJET"

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

SQL_FILE = os.path.join(BASE_DIR, "schemas_oracle.sql")
BATCH_SIZE = 1000
COMMIT_FREQUENCY = 10

# ============================================================================
# ÉTAPE 0 : AUDIT DES DONNÉES POSTGRESQL
# ============================================================================

def step_0_audit_postgresql():
    """Audit et vérification intégrité PostgreSQL"""
    print("\n" + "="*80)
    print("ÉTAPE 0 : AUDIT DES DONNÉES POSTGRESQL")
    print("="*80 + "\n")
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # Vérifier les violations NULL
        print("Vérification des valeurs NULL dans colonnes NOT NULL...\n")
        
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        
        all_tables = [row[0] for row in cursor.fetchall()]
        errors_found = False
        
        for table in all_tables:
            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
                AND is_nullable = 'NO'
            """, (table,))
            
            nn_cols = [row[0] for row in cursor.fetchall()]
            
            for col in nn_cols:
                cursor.execute(f'SELECT COUNT(*) FROM "{table}" WHERE "{col}" IS NULL')
                count = cursor.fetchone()[0]
                
                if count > 0:
                    errors_found = True
                    print(f"❌ Table '{table}' - Colonne '{col}' : {count} NULL trouvé(s)")
        
        if not errors_found:
            print("✅ Aucune violation NULL détectée")
        else:
            print("\n⚠️ CORRECTION RECOMMANDÉE :")
            print("Veuillez corriger les valeurs NULL avant de continuer la migration")
            cursor.close()
            conn.close()
            return False
        
        # Compter les lignes par table
        print("\nComptage des lignes par table...\n")
        
        total_rows = 0
        for table in all_tables:
            cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
            count = cursor.fetchone()[0]
            if count > 0:
                print(f" {table:40} : {count:>10,} lignes")
                total_rows += count
        
        print(f"\n✅ Total : {total_rows:,} lignes à migrer")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ ERREUR AUDIT : {e}")
        return False

# ============================================================================
# ÉTAPE 1 : GÉNÉRATION DU DDL ORACLE
# ============================================================================

def step_1_generate_ddl():
    """Génère le DDL SQL Oracle"""
    print("\n" + "="*80)
    print("ÉTAPE 1 : GÉNÉRATION DU DDL ORACLE")
    print("="*80 + "\n")
    
    try:
        print(f"Exécution : python generate_migration.py\n")
        
        result = subprocess.run(
            [sys.executable, os.path.join(BASE_DIR, "generate_migration.py")],
            cwd=BASE_DIR,
            capture_output=True,
            text=True
        )
        
        print(result.stdout)
        
        if result.returncode != 0:
            print(f"❌ ERREUR : {result.stderr}")
            return False
        
        if not os.path.exists(SQL_FILE):
            print(f"❌ Fichier DDL non créé : {SQL_FILE}")
            return False
        
        file_size = os.path.getsize(SQL_FILE)
        print(f"✅ Fichier DDL généré : {file_size:,} octets")
        
        return True
        
    except Exception as e:
        print(f"❌ ERREUR GÉNÉRATION : {e}")
        return False

# ============================================================================
# ÉTAPE 2 : EXÉCUTION DU DDL (CRÉATION TABLES)
# ============================================================================

def step_2_execute_ddl():
    """Exécute le DDL dans Oracle"""
    print("\n" + "="*80)
    print("ÉTAPE 2 : EXÉCUTION DU DDL ORACLE (CRÉATION TABLES)")
    print("="*80 + "\n")
    
    try:
        print(f"Lecture du fichier DDL : {SQL_FILE}\n")
        
        with open(SQL_FILE, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        print(f"Connexion à Oracle...\n")
        
        conn = oracledb.connect(**ORACLE_CONFIG)
        cursor = conn.cursor()
        
        # Diviser en statements individuels
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        success_count = 0
        error_count = 0
        
        print(f"Exécution de {len(statements)} statements SQL...\n")
        
        for i, statement in enumerate(statements, 1):
            # Ignorer les commentaires et les lignes vides
            if statement.startswith('--') or not statement.strip():
                continue
            
            try:
                cursor.execute(statement)
                success_count += 1
                
                # Afficher les DDL de création
                if 'CREATE TABLE' in statement:
                    table_name = statement.split('CREATE TABLE')[1].split('(')[0].strip().strip('"')
                    print(f"  [{i:3d}] ✅ CREATE TABLE : {table_name}")
                    
            except Exception as e:
                error_count += 1
                # Certaines erreurs sont acceptables (ex: table existe déjà)
                if 'already exists' in str(e) or 'ORA-00955' in str(e):
                    print(f"  [{i:3d}] ⚠️ Table existe déjà (ignoré)")
                else:
                    print(f"  [{i:3d}] ❌ Erreur : {str(e)[:60]}")
        
        conn.commit()
        
        print(f"\n✅ Exécution DDL complétée : {success_count} OK, {error_count} erreurs")
        
        # Vérifier les tables créées
        cursor.execute("SELECT COUNT(*) FROM user_tables")
        table_count = cursor.fetchone()[0]
        print(f"✅ Nombre de tables en Oracle : {table_count}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ ERREUR EXÉCUTION DDL : {e}")
        import traceback
        traceback.print_exc()
        return False

# ============================================================================
# ÉTAPE 3 : DÉSACTIVATION CONTRAINTES FK
# ============================================================================

def step_3_disable_fk():
    """Désactive les contraintes FK"""
    print("\n" + "="*80)
    print("ÉTAPE 3 : DÉSACTIVATION DES CONTRAINTES FK")
    print("="*80 + "\n")
    
    try:
        print("Connexion à Oracle...\n")
        
        conn = oracledb.connect(**ORACLE_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT constraint_name, table_name
            FROM user_constraints
            WHERE constraint_type = 'R'
            ORDER BY table_name
        """)
        
        fk_constraints = cursor.fetchall()
        print(f"Désactivation de {len(fk_constraints)} contraintes FK...\n")
        
        for constraint_name, table_name in fk_constraints:
            try:
                cursor.execute(f'ALTER TABLE "{table_name}" DISABLE CONSTRAINT "{constraint_name}"')
                print(f"  ✅ {table_name}.{constraint_name}")
            except Exception as e:
                print(f"  ⚠️ {table_name}.{constraint_name} : {str(e)[:50]}")
        
        conn.commit()
        print(f"\n✅ Contraintes FK désactivées")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ ERREUR DÉSACTIVATION FK : {e}")
        return False

# ============================================================================
# ÉTAPE 4 : MIGRATION DONNÉES
# ============================================================================

def step_4_migrate_data():
    """Migre les données"""
    print("\n" + "="*80)
    print("ÉTAPE 4 : MIGRATION DES DONNÉES")
    print("="*80 + "\n")
    
    try:
        print("Exécution : python migrate_data_complete.py\n")
        
        result = subprocess.run(
            [sys.executable, os.path.join(BASE_DIR, "migrate_data_complete.py")],
            cwd=BASE_DIR,
            capture_output=False,
            text=True,
            input="o\n"  # Répondre "o" automatiquement
        )
        
        if result.returncode != 0:
            print(f"⚠️ Code de sortie : {result.returncode}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ ERREUR MIGRATION : {e}")
        return False

# ============================================================================
# ÉTAPE 5 : RÉACTIVATION CONTRAINTES FK
# ============================================================================

def step_5_enable_fk():
    """Réactive les contraintes FK"""
    print("\n" + "="*80)
    print("ÉTAPE 5 : RÉACTIVATION DES CONTRAINTES FK")
    print("="*80 + "\n")
    
    try:
        print("Connexion à Oracle...\n")
        
        conn = oracledb.connect(**ORACLE_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT constraint_name, table_name
            FROM user_constraints
            WHERE constraint_type = 'R'
            AND status = 'DISABLED'
            ORDER BY table_name
        """)
        
        fk_constraints = cursor.fetchall()
        print(f"Réactivation de {len(fk_constraints)} contraintes FK...\n")
        
        success_count = 0
        error_count = 0
        
        for constraint_name, table_name in fk_constraints:
            try:
                cursor.execute(f'ALTER TABLE "{table_name}" ENABLE CONSTRAINT "{constraint_name}"')
                print(f"  ✅ {table_name}.{constraint_name}")
                success_count += 1
            except Exception as e:
                print(f"  ❌ {table_name}.{constraint_name} : {str(e)[:50]}")
                error_count += 1
        
        conn.commit()
        print(f"\n✅ Réactivation complétée : {success_count} OK, {error_count} erreurs")
        
        if error_count > 0:
            print("\n⚠️ Il y a des violations d'intégrité référentielle")
            print("Vérifiez les données dans PostgreSQL et Oracle")
        
        cursor.close()
        conn.close()
        
        return error_count == 0
        
    except Exception as e:
        print(f"❌ ERREUR RÉACTIVATION : {e}")
        return False

# ============================================================================
# ÉTAPE 6 : RAPPORT FINAL
# ============================================================================

def step_6_final_report():
    """Rapport final de migration"""
    print("\n" + "="*80)
    print("ÉTAPE 6 : RAPPORT FINAL DE MIGRATION")
    print("="*80 + "\n")
    
    try:
        print("Connexion à Oracle...\n")
        
        conn = oracledb.connect(**ORACLE_CONFIG)
        cursor = conn.cursor()
        
        # Compter les tables
        cursor.execute("SELECT COUNT(*) FROM user_tables")
        table_count = cursor.fetchone()[0]
        print(f"📊 Nombre de tables : {table_count}")
        
        # Compter les lignes
        cursor.execute("""
            SELECT table_name, num_rows
            FROM user_tables
            WHERE table_name NOT LIKE 'BIN$%'
            ORDER BY table_name
        """)
        
        tables_info = cursor.fetchall()
        total_rows = sum(row[1] if row[1] else 0 for row in tables_info)
        
        print(f"📊 Nombre total de lignes : {total_rows:,}\n")
        
        print("Détail par table :\n")
        for table_name, num_rows in tables_info:
            if num_rows and num_rows > 0:
                print(f"  {table_name:40} : {num_rows:>10,} lignes")
        
        # Vérifier les contraintes FK
        cursor.execute("""
            SELECT COUNT(*) FROM user_constraints
            WHERE constraint_type = 'R'
            AND status = 'ENABLED'
        """)
        
        enabled_fk = cursor.fetchone()[0]
        print(f"\n✅ Contraintes FK réactivées : {enabled_fk}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*80)
        print("✅ MIGRATION COMPLÈTE AVEC SUCCÈS")
        print("="*80 + "\n")
        
        return True
        
    except Exception as e:
        print(f"❌ ERREUR RAPPORT : {e}")
        return False

# ============================================================================
# FONCTION PRINCIPALE
# ============================================================================

def main():
    """Exécute la migration complète"""
    
    print("\n" + "="*80)
    print("MIGRATION COMPLÈTE POSTGRESQL → ORACLE (VERSION 3 - AUTOMATISÉE)")
    print("="*80)
    print(f"\nDate : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Source : {PG_CONFIG['database']}@{PG_CONFIG['host']}")
    print(f"Cible : {ORACLE_CONFIG['user']}@{ORACLE_CONFIG['dsn']}")
    print("\n" + "="*80)
    
    steps = [
        ("Audit PostgreSQL", step_0_audit_postgresql),
        ("Génération DDL", step_1_generate_ddl),
        ("Exécution DDL", step_2_execute_ddl),
        ("Désactivation FK", step_3_disable_fk),
        ("Migration Données", step_4_migrate_data),
        ("Réactivation FK", step_5_enable_fk),
        ("Rapport Final", step_6_final_report),
    ]
    
    start_time = datetime.now()
    
    for step_name, step_func in steps:
        if not step_func():
            print(f"\n❌ ÉCHEC À L'ÉTAPE : {step_name}")
            return False
    
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"\n✅ MIGRATION COMPLÈTE EN {duration:.2f} secondes")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️ Migration interrompue (Ctrl+C)\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERREUR : {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
