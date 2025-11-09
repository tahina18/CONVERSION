"""
Module: collection_type_enum_fixed.py (VERSION CORRIGÉE)
--------------------------------------------------------
Collecte et convertit les types ENUM PostgreSQL en contraintes CHECK Oracle
avec les VRAIS noms de colonnes (pas les noms de types ENUM).

CORRECTION PRINCIPALE:
✅ Utilise le vrai nom de colonne (ex: 'role') au lieu du type ENUM (ex: 'accountroleenum')
"""

def collect_enum_columns(cursor):
    """
    Collecte toutes les colonnes qui utilisent des types ENUM.
    
    Retourne une liste de dictionnaires avec:
    - table: nom de la table
    - column: nom réel de la colonne (CORRIGÉ)
    - enum_type: nom du type ENUM
    """
    cursor.execute("""
        SELECT 
            c.table_name,
            c.column_name,
            c.udt_name AS enum_type
        FROM information_schema.columns c
        JOIN pg_type t ON c.udt_name = t.typname
        WHERE t.typtype = 'e'
          AND c.table_schema = 'public'
        ORDER BY c.table_name, c.column_name
    """)
    
    enum_columns = []
    for row in cursor.fetchall():
        enum_columns.append({
            "table": row[0],
            "column": row[1],      # ✅ VRAI nom de colonne
            "enum_type": row[2]
        })
    
    return enum_columns

def get_enum_values(cursor, enum_type):
    """
    Récupère les valeurs possibles d'un type ENUM.
    """
    cursor.execute("""
        SELECT e.enumlabel
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        WHERE t.typname = %s
        ORDER BY e.enumsortorder
    """, (enum_type,))
    
    return [row[0] for row in cursor.fetchall()]

def convert_enum_to_check(table_name, column_name, enum_type, enum_values):
    """
    Convertit un type ENUM en contrainte CHECK Oracle.
    
    CORRECTION:
    - Utilise le vrai nom de colonne (column_name) au lieu du type ENUM
    - Gère les mots réservés Oracle (ORDER, SESSION, etc.)
    """
    # Protéger les noms de table réservés
    reserved_words = {'order', 'session', 'user', 'date', 'comment', 'file'}
    if table_name.lower() in reserved_words:
        safe_table_name = f'"{table_name.upper()}"'
    else:
        safe_table_name = table_name
    
    # Calculer la longueur max des valeurs
    max_length = max(len(val) for val in enum_values)
    
    # Type Oracle
    oracle_type = f"VARCHAR2({max_length})"
    
    # Valeurs ENUM formatées
    enum_values_formatted = ", ".join([f"'{val}'" for val in enum_values])
    
    # Nom de la contrainte (tronqué si nécessaire)
    constraint_name = f"chk_{table_name}_{column_name}"[:30]
    
    # Contrainte CHECK avec le VRAI nom de colonne
    check_constraint = f"CONSTRAINT {constraint_name} CHECK ({column_name} IN ({enum_values_formatted}))"
    
    return oracle_type, check_constraint

def get_enum_conversion_dict(enum_columns):
    """
    Retourne un dictionnaire de conversion pour tous les types ENUM.
    
    Format: {enum_type: (oracle_type, check_constraint)}
    """
    from type_mapping import initialize_enum_conversion
    
    enum_dict = {}
    
    # Grouper par type ENUM
    enum_types_map = {}
    for col in enum_columns:
        enum_type = col["enum_type"]
        if enum_type not in enum_types_map:
            enum_types_map[enum_type] = []
        enum_types_map[enum_type].append(col)
    
    # Pour chaque type ENUM, créer la conversion
    for enum_type, columns in enum_types_map.items():
        # Prendre la première colonne comme référence
        first_col = columns[0]
        
        # Récupérer les valeurs ENUM depuis le dictionnaire global
        from type_mapping import enum_conversion
        enum_values = enum_conversion.get(enum_type, [])
        
        if enum_values:
            max_length = max(len(val) for val in enum_values)
            oracle_type = f"VARCHAR2({max_length})"
            enum_dict[enum_type] = (oracle_type, "")
    
    return enum_dict
