"""
Module: generate_ddl_v2.py (VERSION 2 - FINALE)
------------------------------------------------
Génère les scripts DDL Oracle avec contraintes CHECK correctes.

CORRECTIONS:
✅ Utilise collection_type_enum_fixed.py
✅ Génère les contraintes CHECK avec les vrais noms de colonnes
"""

import psycopg2
import re

# Import des modules corrigés
from type_mapping import (
    convert_type, 
    initialize_enum_conversion, 
    clean_default_value,
    quote_identifier_if_needed
)

from collection_type_enum import collect_enum_columns, get_enum_values, convert_enum_to_check

def truncate_constraint_name(name, max_length=30):
    """Tronque un nom de contrainte pour Oracle"""
    if len(name) <= max_length:
        return name
    return name[:max_length]

def generate_tables(connection_params):
    """Génère les CREATE TABLE"""
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    
    initialize_enum_conversion(cursor)
    enum_columns = collect_enum_columns(cursor)
    
    # Créer un mapping type_enum -> VARCHAR2 type
    enum_mapping = {}
    for col in enum_columns:
        enum_type = col["enum_type"]
        if enum_type not in enum_mapping:
            enum_values = get_enum_values(cursor, enum_type)
            max_length = max(len(val) for val in enum_values) if enum_values else 255
            enum_mapping[enum_type] = f"VARCHAR2({max_length})"
    
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    tables = [row[0] for row in cursor.fetchall()]
    
    print("-- CRÉATION DES TABLES")
    print()
    
    for table in tables:
        safe_table_name = quote_identifier_if_needed(table)
        
        cursor.execute("""
            SELECT 
                column_name, udt_name, data_type, 
                character_maximum_length, column_default,
                numeric_precision, numeric_scale, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table,))
        columns = cursor.fetchall()
        
        print(f"CREATE TABLE {safe_table_name} (")
        
        col_defs = []
        for row in columns:
            column_name, udt_name, data_type = row[0], row[1], row[2]
            char_max_len, col_default = row[3], row[4]
            num_precision, num_scale, is_nullable = row[5], row[6], row[7]
            
            safe_column_name = quote_identifier_if_needed(column_name)
            
            # Type Oracle
            if udt_name in enum_mapping:
                oracle_type = enum_mapping[udt_name]
            elif col_default and 'nextval(' in str(col_default):
                oracle_type = convert_type('serial')
            elif data_type in ('numeric', 'decimal') and num_precision:
                oracle_type = f"NUMBER({num_precision},{num_scale or 0})"
            elif char_max_len and data_type in ('character varying', 'varchar'):
                oracle_type = f"VARCHAR2({char_max_len})"
            elif data_type == 'uuid':
                oracle_type = 'VARCHAR2(36)'
            elif data_type in ('jsonb', 'json'):
                oracle_type = 'CLOB'
            elif data_type == 'boolean':
                oracle_type = 'NUMBER(1)'
            else:
                oracle_type = convert_type(data_type)
            
            # DEFAULT
            default_clause = ""
            if col_default and 'nextval(' not in str(col_default):
                cleaned = clean_default_value(col_default)
                if cleaned:
                    default_clause = f" DEFAULT {cleaned}"
            
            nullable = "" if is_nullable == 'YES' else " NOT NULL"
            
            col_defs.append(f"  {safe_column_name} {oracle_type}{default_clause}{nullable}")
        
        print(",\n".join(col_defs))
        print(");")
        print()
    
    cursor.close()
    conn.close()

def generate_constraints(connection_params):
    """Génère les contraintes PRIMARY KEY, FOREIGN KEY, UNIQUE"""
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    tables = [row[0] for row in cursor.fetchall()]
    
    print("-- CONTRAINTES")
    print()
    
    for table in tables:
        safe_table_name = quote_identifier_if_needed(table)
        
        cursor.execute("""
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = %s AND table_schema = 'public'
        """, (table,))
        constraints = cursor.fetchall()
        
        for const_name, const_type in constraints:
            oracle_const_name = truncate_constraint_name(const_name)
            
            cursor.execute("""
                SELECT column_name
                FROM information_schema.key_column_usage
                WHERE constraint_name = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (const_name, table))
            columns = [quote_identifier_if_needed(row[0]) for row in cursor.fetchall()]
            columns_formatted = ", ".join(columns) if columns else ""
            
            if const_type == "PRIMARY KEY" and columns_formatted:
                print(f"ALTER TABLE {safe_table_name} ADD CONSTRAINT {oracle_const_name} PRIMARY KEY ({columns_formatted});")
            
            elif const_type == "FOREIGN KEY" and columns_formatted:
                cursor.execute("""
                    SELECT ccu.table_name, ccu.column_name
                    FROM information_schema.constraint_column_usage AS ccu
                    WHERE ccu.constraint_name = %s
                """, (const_name,))
                fk_info = cursor.fetchone()
                if fk_info:
                    foreign_table = quote_identifier_if_needed(fk_info[0])
                    foreign_column = quote_identifier_if_needed(fk_info[1])
                    print(f"ALTER TABLE {safe_table_name} ADD CONSTRAINT {oracle_const_name} FOREIGN KEY ({columns_formatted}) REFERENCES {foreign_table}({foreign_column});")
            
            elif const_type == "UNIQUE" and columns_formatted:
                print(f"ALTER TABLE {safe_table_name} ADD CONSTRAINT {oracle_const_name} UNIQUE ({columns_formatted});")
    
    print()
    cursor.close()
    conn.close()

def generate_enum_checks(connection_params):
    """Génère les contraintes CHECK pour les ENUM avec les VRAIS noms de colonnes"""
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    
    initialize_enum_conversion(cursor)
    enum_columns = collect_enum_columns(cursor)
    
    print("-- CONTRAINTES CHECK POUR LES TYPES ENUM")
    print()
    
    for col in enum_columns:
        table_name = col["table"]
        column_name = col["column"]  # ✅ VRAI nom de colonne
        enum_type = col["enum_type"]
        
        # Récupérer les valeurs ENUM
        enum_values = get_enum_values(cursor, enum_type)
        
        if enum_values:
            # Générer la contrainte CHECK avec le vrai nom de colonne
            oracle_type, check_constraint = convert_enum_to_check(
                table_name, column_name, enum_type, enum_values
            )
            
            if check_constraint:
                safe_table_name = quote_identifier_if_needed(table_name)
                print(f"ALTER TABLE {safe_table_name} ADD {check_constraint};")
    
    print()
    cursor.close()
    conn.close()

def generate_complete_migration(connection_params):
    """Génère la migration complète"""
    print()
    print("-- ============================================================================")
    print("-- MIGRATION POSTGRESQL → ORACLE (VERSION 2 - FINALE)")
    print("-- ============================================================================")
    print()
    
    generate_tables(connection_params)
    generate_constraints(connection_params)
    generate_enum_checks(connection_params)
    
    print("-- ============================================================================")
    print("-- FIN DE LA MIGRATION")
    print("-- ============================================================================")
