# -*- coding: utf-8 -*-
"""
Script: detect_and_fix_nulls.py

DIAGNOSTIC & CORRECTION DES PROBLÈMES NULL

Détecte :
✅ Toutes les colonnes NOT NULL dans PostgreSQL
✅ Quelles colonnes ont des NULL
✅ Propose des valeurs par défaut
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

# ============================================================================
# DÉTECTION DES PROBLÈMES NULL
# ============================================================================

def detect_null_problems():
    """
    Détecte les colonnes NOT NULL avec des valeurs NULL dans PostgreSQL
    """
    print("\n" + "="*80)
    print("DÉTECTION DES PROBLÈMES NULL")
    print("="*80 + "\n")
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # Récupérer toutes les tables
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"Analyse de {len(tables)} tables...\n")
        
        problems = []
        
        for table in tables:
            # Récupérer les colonnes NOT NULL
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s
                AND table_schema = 'public'
                AND is_nullable = 'NO'
                ORDER BY ordinal_position
            """, (table,))
            
            not_null_cols = cursor.fetchall()
            
            if not not_null_cols:
                continue
            
            # Vérifier s'il y a des NULL
            for col_name, col_type in not_null_cols:
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{table}" WHERE "{col_name}" IS NULL'
                )
                null_count = cursor.fetchone()[0]
                
                if null_count > 0:
                    problems.append({
                        'table': table,
                        'column': col_name,
                        'type': col_type,
                        'null_count': null_count
                    })
        
        cursor.close()
        conn.close()
        
        if not problems:
            print("✅ AUCUN PROBLÈME NULL DÉTECTÉ !\n")
            return None
        
        # Afficher les problèmes
        print(f"❌ {len(problems)} PROBLÈME(S) NULL DÉTECTÉ(S)\n")
        
        print("="*80)
        print("DÉTAIL DES PROBLÈMES")
        print("="*80 + "\n")
        
        for problem in problems:
            print(f"Table    : {problem['table']}")
            print(f"Colonne  : {problem['column']}")
            print(f"Type     : {problem['type']}")
            print(f"NULL     : {problem['null_count']} lignes")
            print()
        
        return problems
        
    except Exception as e:
        print(f"❌ ERREUR : {e}\n")
        import traceback
        traceback.print_exc()
        return None

# ============================================================================
# CORRECTION DES NULL
# ============================================================================

def suggest_and_fix_nulls(problems):
    """
    Propose et applique les corrections
    """
    print("\n" + "="*80)
    print("CORRECTION DES NULL")
    print("="*80 + "\n")
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # Mapping des valeurs par défaut par type
        DEFAULT_VALUES = {
            'character varying': "'N/A'",
            'character': "'N/A'",
            'text': "'N/A'",
            'integer': "0",
            'bigint': "0",
            'numeric': "0",
            'double precision': "0",
            'boolean': "false",
            'date': "CURRENT_DATE",
            'timestamp without time zone': "CURRENT_TIMESTAMP",
            'timestamp with time zone': "CURRENT_TIMESTAMP",
            'uuid': "'00000000-0000-0000-0000-000000000000'",
            'json': "'{}'::json",
            'jsonb': "'{}'::jsonb",
        }
        
        fixed_count = 0
        
        for problem in problems:
            table = problem['table']
            column = problem['column']
            col_type = problem['type']
            null_count = problem['null_count']
            
            # Chercher la valeur par défaut
            default_value = None
            
            for key, value in DEFAULT_VALUES.items():
                if key.lower() in col_type.lower():
                    default_value = value
                    break
            
            if default_value is None:
                print(f"⚠️ {table}.{column} - Type '{col_type}' : pas de valeur par défaut")
                continue
            
            # Appliquer la correction
            try:
                update_query = f'UPDATE "{table}" SET "{column}" = {default_value} WHERE "{column}" IS NULL'
                
                print(f"▶ Correction : {table}.{column}")
                print(f"   Query : {update_query}")
                
                cursor.execute(update_query)
                updated = cursor.rowcount
                
                conn.commit()
                
                print(f"   ✅ {updated} ligne(s) corrigée(s)\n")
                
                fixed_count += 1
                
            except Exception as e:
                print(f"   ❌ Erreur : {str(e)[:70]}\n")
                conn.rollback()
        
        cursor.close()
        conn.close()
        
        print("="*80)
        print(f"✅ {fixed_count} colonne(s) corrigée(s)\n")
        
        return True
        
    except Exception as e:
        print(f"❌ ERREUR : {e}\n")
        return False

# ============================================================================
# FONCTION PRINCIPALE
# ============================================================================

def main():
    print("\n" + "="*80)
    print("DÉTECTION & CORRECTION DES PROBLÈMES NULL")
    print("PostgreSQL")
    print("="*80)
    print(f"\nConnexion : {PG_CONFIG['database']}@{PG_CONFIG['host']}")
    
    # ÉTAPE 1 : Détecter
    problems = detect_null_problems()
    
    if problems is None:
        print("\n✅ Aucun problème détecté. Vous pouvez lancer la migration !\n")
        return
    
    # ÉTAPE 2 : Corriger
    response = input("\n▶ Appliquer les corrections ? (o/n) : ").strip().lower()
    
    if response != 'o':
        print("\n⚠️ Corrections non appliquées.\n")
        return
    
    if suggest_and_fix_nulls(problems):
        print("✅ CORRECTION COMPLÈTE")
        print("\nVous pouvez maintenant relancer la migration !\n")
    else:
        print("❌ ERREUR LORS DES CORRECTIONS\n")

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
