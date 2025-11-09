def get_problematic_not_null_columns(oracle_conn, table_name):
    """
    Identifie les colonnes NOT NULL dans Oracle pour une table donnée.
    """
    cursor = oracle_conn.cursor()
    cursor.execute("""
        SELECT column_name
        FROM user_tab_columns
        WHERE table_name = :table_name AND nullable = 'N'
    """, {'table_name': table_name.upper()})
    not_null_cols = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return not_null_cols

def disable_not_null_constraints(oracle_conn, table_name, columns):
    """
    Désactive temporairement la contrainte NOT NULL en modifiant la colonne pour accepter NULL.
    """
    cursor = oracle_conn.cursor()
    for col in columns:
        sql = f'ALTER TABLE "{table_name}" MODIFY ("{col}" NULL)'
        cursor.execute(sql)
    cursor.close()
    oracle_conn.commit()

def enable_not_null_constraints(oracle_conn, table_name, columns):
    """
    Réactive la contrainte NOT NULL en modifiant la colonne pour l’appliquer.
    """
    cursor = oracle_conn.cursor()
    for col in columns:
        sql = f'ALTER TABLE "{table_name}" MODIFY ("{col}" NOT NULL)'
        cursor.execute(sql)
    cursor.close()
    oracle_conn.commit()
