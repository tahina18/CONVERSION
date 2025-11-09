import psycopg2
import oracledb
from datetime import datetime

# Configuration des connexions
pg_conn = psycopg2.connect(
    host="source_host",
    database="AURA",
    user="postgres",
    password="admin"
)

oracle_conn = oracledb.connect(
    user="C##TEST",
    password="admin",
    dsn="localhost:1521/PROJET"
)

pg_cursor = pg_conn.cursor()
oracle_cursor = oracle_conn.cursor()

# Récupérer les métadonnées de la table source
def get_source_columns(table_name):
    """Récupère les colonnes de PostgreSQL sans vérifier les types Oracle"""
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position"
    pg_cursor.execute(query, (table_name,))
    return [row[0] for row in pg_cursor.fetchall()]

# Migration sans conversion de type
def migrate_data(source_table, target_table):
    """
    Copie les données de la source sans forcer de conversion
    Ignore complètement les types de données Oracle
    """
    try:
        # Récupérer les colonnes
        columns = get_source_columns(source_table)
        col_list = ', '.join(f'"{col}"' for col in columns)
        
        # Lire TOUTES les données de la source
        select_query = f'SELECT {col_list} FROM "{source_table}"'
        pg_cursor.execute(select_query)
        
        rows = pg_cursor.fetchall()
        total_rows = len(rows)
        
        if total_rows == 0:
            print(f"⚠️  Aucune donnée à copier de {source_table}")
            return 0
        
        # Construire l'INSERT Oracle dynamiquement
        placeholders = ', '.join([':' + str(i+1) for i in range(len(columns))])
        insert_query = f'INSERT INTO "{target_table}" ({col_list}) VALUES ({placeholders})'
        
        # Insérer par batch pour éviter les timeouts
        batch_size = 1000
        inserted_count = 0
        
        for i in range(0, total_rows, batch_size):
            batch = rows[i:i+batch_size]
            
            try:
                # Convertir les None et types spéciaux, mais garder les valeurs originales
                cleaned_batch = []
                for row in batch:
                    cleaned_row = []
                    for val in row:
                        if val is None:
                            cleaned_row.append(None)
                        elif isinstance(val, (list, dict)):
                            # JSON/JSONB - garder comme string
                            cleaned_row.append(str(val))
                        elif isinstance(val, bool):
                            # Boolean - garder comme 0/1
                            cleaned_row.append(1 if val else 0)
                        else:
                            # Tous les autres types - garder originaux
                            cleaned_row.append(val)
                    cleaned_batch.append(tuple(cleaned_row))
                
                # Insérer le batch
                oracle_cursor.executemany(insert_query, cleaned_batch)
                inserted_count += len(cleaned_batch)
                
                print(f"✓ {inserted_count}/{total_rows} lignes insérées ({(inserted_count/total_rows)*100:.1f}%)")
                
            except Exception as e:
                print(f"❌ Erreur lors de l'insertion du batch {i//batch_size + 1}")
                print(f"   Détail: {str(e)}")
                print(f"   Nombre de lignes avant erreur: {inserted_count}")
                raise
        
        oracle_conn.commit()
        print(f"✓ Migration complète: {inserted_count} lignes copiées")
        return inserted_count
        
    except Exception as e:
        print(f"❌ Erreur de migration: {str(e)}")
        oracle_conn.rollback()
        raise

# Utilisation
if __name__ == "__main__":
    tables_to_migrate = [
        ("public.configuration", "CONFIGURATION"),  # source -> target
        ("public.users", "USERS"),
    ]
    
    for source_table, target_table in tables_to_migrate:
        print(f"\n🔄 Migration: {source_table} → {target_table}")
        migrate_data(source_table, target_table)
    
    pg_cursor.close()
    oracle_cursor.close()
    pg_conn.close()
    oracle_conn.close()
