import oracledb

# Configuration de la connexion Oracle
ORACLE_CONFIG = {
    'user': 'C##TEST',
    'password': 'admin',
    'dsn': 'localhost:1521/PROJET'
}

def run_sql(cursor, sql):
    try:
        cursor.execute(sql)
        print(f"OK: {sql}")
    except Exception as e:
        print(f"Erreur lors de l'exécution de: {sql}\n  → {e}")

def drop_all_objects():
    conn = oracledb.connect(**ORACLE_CONFIG)
    cursor = conn.cursor()

    print("Désactivation des contraintes Foreign Key...")
    cursor.execute("""
        SELECT constraint_name, table_name
        FROM user_constraints
        WHERE constraint_type = 'R'
        ORDER BY table_name
    """)
    fk_constraints = cursor.fetchall()
    for constraint_name, table_name in fk_constraints:
        run_sql(cursor, f'ALTER TABLE "{table_name}" DISABLE CONSTRAINT "{constraint_name}"')
    conn.commit()

    print("Suppression des vues...")
    cursor.execute("SELECT view_name FROM user_views")
    for (view_name,) in cursor.fetchall():
        run_sql(cursor, f'DROP VIEW "{view_name}" CASCADE CONSTRAINTS')

    print("Suppression des tables...")
    cursor.execute("SELECT table_name FROM user_tables")
    for (table_name,) in cursor.fetchall():
        run_sql(cursor, f'DROP TABLE "{table_name}" CASCADE CONSTRAINTS PURGE')

    print("Suppression des séquences utilisateur...")
    cursor.execute("SELECT sequence_name FROM user_sequences WHERE sequence_name NOT LIKE 'SYS_LOB%'")
    for (seq_name,) in cursor.fetchall():
        run_sql(cursor, f'DROP SEQUENCE "{seq_name}"')

    print("Suppression des synonymes...")
    cursor.execute("SELECT synonym_name FROM user_synonyms")
    for (syn_name,) in cursor.fetchall():
        run_sql(cursor, f'DROP SYNONYM "{syn_name}"')

    print("Suppression des triggers...")
    cursor.execute("SELECT trigger_name FROM user_triggers")
    for (trigger_name,) in cursor.fetchall():
        run_sql(cursor, f'DROP TRIGGER "{trigger_name}"')

    print("Suppression des procédures et fonctions...")
    cursor.execute("""
        SELECT object_name, object_type
        FROM user_objects
        WHERE object_type IN ('PROCEDURE', 'FUNCTION')
    """)
    for object_name, object_type in cursor.fetchall():
        run_sql(cursor, f'DROP {object_type} "{object_name}"')

    print("Réactivation des contraintes Foreign Key...")
    for constraint_name, table_name in fk_constraints:
        run_sql(cursor, f'ALTER TABLE "{table_name}" ENABLE CONSTRAINT "{constraint_name}"')
    conn.commit()

    cursor.close()
    conn.close()
    print("Suppression complète terminée.")

if __name__ == "__main__":
    drop_all_objects()
