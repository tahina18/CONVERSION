import unittest
import psycopg2
from collection_type_enum import collect_enum_columns, get_enum_conversion_dict
from generate_ddl import generate_ddl, generate_constraints, generate_enum_ddl, generate_manual_sequences_ddl
from collection_sequence import get_sequence_types
from collection_triggers import generate_triggers_ddl
from collection_indexes import generate_indexes_ddl
from collections_views import generate_views_ddl  
from collections_functions_procedures import generate_functions_procedures_ddl  


class TestWithRealDatabase(unittest.TestCase):
    
    def test_real_database_extract(self):
        connection_params = {
            "host": "localhost",
            "port": 5432,
            "database": "AURA",
            "user": "postgres",
            "password": "admin"
        }
        
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        try:
            # =====================================
            # ÉTAPE 1 : GÉNÉRATION DES TABLES
            # =====================================
            print("\n" + "="*80)
            print("ÉTAPE 1 : GÉNÉRATION DES TABLES")
            print("="*80 + "\n")
            generate_ddl(connection_params)
            
            # =====================================
            # ÉTAPE 2 : GÉNÉRATION DES CONTRAINTES
            # =====================================
            print("\n" + "="*80)
            print("ÉTAPE 2 : GÉNÉRATION DES CONTRAINTES")
            print("="*80 + "\n")
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")
            tables = [row[0] for row in cursor.fetchall()]
            for table in tables:
                print(f"\n--- Contraintes pour la table : {table} ---")
                generate_constraints(cursor, table)
            
            # =====================================
            # ÉTAPE 3 : GÉNÉRATION DES ENUM
            # =====================================
            print("\n" + "="*80)
            print("ÉTAPE 3 : GÉNÉRATION DES ENUM")
            print("="*80 + "\n")
            generate_enum_ddl(cursor)
            
            # =====================================
            # ÉTAPE 4 : GÉNÉRATION DES SÉQUENCES
            # =====================================
            print("\n" + "="*80)
            print("ÉTAPE 4 : GÉNÉRATION DES SÉQUENCES")
            print("="*80 + "\n")
            sequences = get_sequence_types(connection_params)
            print("Liste des séquences dans la base PostgreSQL avec leur type :")
            for seq in sequences:
                print(f"Séquence: {seq['sequence_name']} - Type: {seq['type']} - Table: {seq['table_name']} - Colonne: {seq['column_name']}")
            generate_manual_sequences_ddl(connection_params)
            
            # =====================================
            # ÉTAPE 5 : GÉNÉRATION DES INDEX
            # =====================================
            print("\n" + "="*80)
            print("ÉTAPE 5 : GÉNÉRATION DES INDEX")
            print("="*80 + "\n")
            generate_indexes_ddl(connection_params)
            
            # =====================================
            # ÉTAPE 6 : GÉNÉRATION DES TRIGGERS
            # =====================================
            print("\n" + "="*80)
            print("ÉTAPE 6 : GÉNÉRATION DES TRIGGERS")
            print("="*80 + "\n")
            generate_triggers_ddl(connection_params)
            
            # =====================================
            # ÉTAPE 7 : GÉNÉRATION DES VUES
            # =====================================
            print("\n" + "="*80)
            print("ÉTAPE 7 : GÉNÉRATION DES VUES")
            print("="*80 + "\n")
            generate_views_ddl(connection_params)  
            # =====================================
            # ÉTAPE 8 : GÉNÉRATION DES FONCTIONS ET PROCÉDURES
            # =====================================
            print("\n" + "="*80)
            print("ÉTAPE 8 : GÉNÉRATION DES FONCTIONS ET PROCÉDURES")
            print("="*80 + "\n")
            generate_functions_procedures_ddl(connection_params)

            
        except Exception as e:
            self.fail(f"Erreur lors de la génération DDL : {e}")
        finally:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    unittest.main()
