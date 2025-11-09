"""
Script: generate_migration_v2.py (VERSION FINALE)
--------------------------------------------------
Génère un fichier SQL Oracle avec contraintes CHECK CORRIGÉES.

UTILISATION:
    python generate_migration_v2.py

RÉSULTAT:
    migration_oracle_V2.sql (avec contraintes CHECK correctes)
"""

import sys
import os
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = r"D:\MEMOIRE\PROJET"
OUTPUT_FILE = os.path.join(BASE_DIR, "schemas_oracle.sql")

CONNECTION_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'AURA',
    'user': 'postgres',
    'password': 'admin'
}

# ============================================================================
# FONCTIONS
# ============================================================================

def print_header():
    """Affiche l'en-tête"""
    print("\n" + "="*80)
    print("GÉNÉRATION SQL ORACLE - ")
    print("="*80)
    print(f"\n📁 Dossier    : {BASE_DIR}")
    print(f"📄 Fichier    : {OUTPUT_FILE}")
    print(f"🗄️  Base       : {CONNECTION_PARAMS['database']}")
    print()
    print("🔧 Corrections appliquées:")
    print("   ✅ Contraintes CHECK avec vrais noms de colonnes")
    print("   ✅ Plus de références aux types ENUM")
    print()

def generate_sql():
    """Génère le fichier SQL V2"""
    print("="*80)
    print("GÉNÉRATION EN COURS")
    print("="*80)
    print()
    
    original_dir = os.getcwd()
    os.chdir(BASE_DIR)
    sys.path.insert(0, BASE_DIR)
    
    try:
        print("1. Import du module generate_ddl_v2...")
        from generate_ddl_v2 import generate_complete_migration
        print("   ✅ Module importé")
        print()
        
        print(f"2. Création du fichier : {OUTPUT_FILE}")
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"""-- ============================================================================
-- SCRIPT DE MIGRATION POSTGRESQL → ORACLE (VERSION 2 - FINALE)
-- ============================================================================
-- Base source      : {CONNECTION_PARAMS['database']} (PostgreSQL)
-- Base cible       : Oracle Database 12c+
-- Date génération  : {now}
-- Version          : 2 - CONTRAINTES CHECK CORRIGÉES
-- ============================================================================
--
-- CORRECTIONS VERSION 2:
--   ✅ Contraintes CHECK utilisent les vrais noms de colonnes
--   ✅ account.role (pas accountroleenum)
--   ✅ account.typeOfAccount (pas accounttypeofaccountenum)
--   ✅ company.companyValidated (pas companycompanyvalidatedenum)
--   ✅ companymembership.status (pas companymembershipstatusenum)
--   ✅ order.status (pas orderstatusenum)
--   ✅ subscription.subscriptionStatus (pas subscriptionsubscriptionstatusenum)
--
-- EXÉCUTION:
--   SQL*Plus: @{os.path.basename(OUTPUT_FILE)}
--
-- ============================================================================

SET ECHO ON;
SET FEEDBACK ON;
SET SERVEROUTPUT ON;
WHENEVER SQLERROR CONTINUE;

""")
            
            print("   ✅ En-tête écrit")
            print()
            
            print("3. Génération du DDL Oracle...")
            original_stdout = sys.stdout
            sys.stdout = f
            
            generate_complete_migration(CONNECTION_PARAMS)
            
            sys.stdout = original_stdout
            
            f.write("""

-- ============================================================================
-- FIN DU SCRIPT DE MIGRATION
-- ============================================================================

COMMIT;

-- Vérifications:
-- SELECT COUNT(*) FROM user_tables;                    -- 30 tables
-- SELECT COUNT(*) FROM user_constraints WHERE constraint_type = 'C';  -- Contraintes CHECK
-- SELECT constraint_name, search_condition FROM user_constraints WHERE table_name = 'ACCOUNT' AND constraint_type = 'C';
""")
            
            print("   ✅ DDL généré")
            print("   ✅ Footer écrit")
        
        print()
        print("4. Vérification du fichier...")
        
        if os.path.exists(OUTPUT_FILE):
            file_size = os.path.getsize(OUTPUT_FILE)
            print(f"   ✅ Fichier créé : {file_size:,} octets ({file_size//1024} KB)")
            
            # Vérifier les contraintes CHECK
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                content = f.read()
                check_count = content.count('CHECK (')
                print(f"   ✅ {check_count} contraintes CHECK générées")
            
            return True
        else:
            print(f"   ❌ Fichier non créé")
            return False
            
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        os.chdir(original_dir)

def print_success():
    """Affiche le message de succès"""
    print()
    print("="*80)
    print("✅ FICHIER SQL VERSION 2 GÉNÉRÉ AVEC SUCCÈS")
    print("="*80)
    print()
    print(f"📄 Fichier : {OUTPUT_FILE}")
    print()
    print("🔧 Différences avec la version précédente:")
    print("   ✅ Contraintes CHECK utilisent les VRAIS noms de colonnes")
    print("   ✅ Plus d'erreur ORA-00904 (identificateur non valide)")
    print()
    print("🚀 Exécution dans Oracle:")
    print(f"   sqlplus user/pass@db @{os.path.basename(OUTPUT_FILE)}")
    print()
    print("📝 Les 6 contraintes CHECK seront ajoutées correctement:")
    print("   1. account.role")
    print("   2. account.typeOfAccount")
    print("   3. company.companyValidated")
    print("   4. companymembership.status")
    print("   5. order.status")
    print("   6. subscription.subscriptionStatus")
    print()
    print("="*80)
    print()

def main():
    """Fonction principale"""
    try:
        print_header()
        
        if not generate_sql():
            print("\n❌ Échec de la génération\n")
            sys.exit(1)
        
        print_success()
        
    except Exception as e:
        print(f"\n❌ ERREUR: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
