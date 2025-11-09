import oracledb

ORACLE_CONFIG = {
    'user': 'C##TEST',
    'password': 'admin',
    'dsn': 'localhost:1521/PROJET'
}

def run_sql(cursor, sql):
    """Exécute une requête SQL avec gestion d'erreur"""
    try:
        cursor.execute(sql)
        print(f"OK: {sql}")
    except Exception as e:
        print(f"Erreur lors de l'exécution de {sql} : {e}")

def enable_fk_constraints():
    """Récupère et réactive toutes les contraintes FK"""
    conn = oracledb.connect(**ORACLE_CONFIG)
    cursor = conn.cursor()
    
    # Récupérer toutes les contraintes de type Foreign Key (R = Referential)
    cursor.execute("""
        SELECT constraint_name, table_name
        FROM user_constraints
        WHERE constraint_type = 'R'
    """)
    
    fk_constraints = cursor.fetchall()
    print(f"Réactivation de {len(fk_constraints)} contraintes FK...")
    
    # Activer chaque contrainte
    for constraint_name, table_name in fk_constraints:
        run_sql(cursor, f'ALTER TABLE "{table_name}" ENABLE CONSTRAINT "{constraint_name}"')
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Toutes les contraintes FK ont été réactivées.")

if __name__ == "__main__":
    enable_fk_constraints()
