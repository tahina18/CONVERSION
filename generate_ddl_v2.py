"""
Module: generate_ddl_v2.py (VERSION 3 - PRESERVE CASE)
------------------------------------------------
Génère les scripts DDL Oracle avec contraintes CHECK correctes.
Respecte la casse exacte des noms d'objets source.

CORRECTIONS:
✅ Préserve la casse des tables et colonnes
✅ Utilise quote_identifier pour tous les identifiants
✅ Génère les contraintes CHECK avec les vrais noms de colonnes
✅ Respecte la casse dans les contraintes et index
"""

import psycopg2
import re

# Import des modules
from type_mapping import (
    convert_type,
    initialize_enum_conversion,
    clean_default_value
)

from collection_type_enum import collect_enum_columns, get_enum_values, convert_enum_to_check

def quote_identifier(name):
    """
    Entoure l'identifiant de guillemets doubles pour préserver la casse
    dans Oracle. Les guillemets doubles permettent à Oracle de respecter
    la casse exacte du nom d'objet.
    """
    if name is None:
        return None
    return f'"{name}"'

def truncate_constraint_name(name, max_length=30):
    """Tronque un nom de contrainte pour Oracle (max 30 caractères)"""
    if len(name) <= max_length:
        return name
    return name[:max_length]

def generate_tables(connection_params):
    """Génère les CREATE TABLE avec préservation de la casse"""
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
        # ✅ PRÉSERVER LA CASSE avec guillemets doubles
        safe_table_name = quote_identifier(table)

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

            # ✅ PRÉSERVER LA CASSE de la colonne
            safe_column_name = quote_identifier(column_name)

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

            col_defs.append(f" {safe_column_name} {oracle_type}{default_clause}{nullable}")

        print(",\n".join(col_defs))
        print(");")
        print()

    cursor.close()
    conn.close()

def generate_constraints(connection_params):
    """Génère les contraintes PRIMARY KEY, FOREIGN KEY, UNIQUE avec préservation de casse"""
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
        # ✅ PRÉSERVER LA CASSE
        safe_table_name = quote_identifier(table)

        cursor.execute("""
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = %s AND table_schema = 'public'
        """, (table,))

        constraints = cursor.fetchall()

        for const_name, const_type in constraints:
            # ✅ PRÉSERVER LA CASSE du nom de contrainte
            oracle_const_name = truncate_constraint_name(const_name)
            safe_const_name = quote_identifier(oracle_const_name)

            cursor.execute("""
                SELECT column_name
                FROM information_schema.key_column_usage
                WHERE constraint_name = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (const_name, table))

            # ✅ PRÉSERVER LA CASSE des colonnes
            columns = [quote_identifier(row[0]) for row in cursor.fetchall()]
            columns_formatted = ", ".join(columns) if columns else ""

            if const_type == "PRIMARY KEY" and columns_formatted:
                print(f"ALTER TABLE {safe_table_name} ADD CONSTRAINT {safe_const_name} PRIMARY KEY ({columns_formatted});")

            elif const_type == "FOREIGN KEY" and columns_formatted:
                cursor.execute("""
                    SELECT ccu.table_name, ccu.column_name
                    FROM information_schema.constraint_column_usage AS ccu
                    WHERE ccu.constraint_name = %s
                """, (const_name,))

                fk_info = cursor.fetchone()
                if fk_info:
                    # ✅ PRÉSERVER LA CASSE des tables et colonnes FK
                    foreign_table = quote_identifier(fk_info[0])
                    foreign_column = quote_identifier(fk_info[1])
                    print(f"ALTER TABLE {safe_table_name} ADD CONSTRAINT {safe_const_name} FOREIGN KEY ({columns_formatted}) REFERENCES {foreign_table}({foreign_column});")

            elif const_type == "UNIQUE" and columns_formatted:
                print(f"ALTER TABLE {safe_table_name} ADD CONSTRAINT {safe_const_name} UNIQUE ({columns_formatted});")

        print()

    cursor.close()
    conn.close()

def generate_enum_checks(connection_params):
    """Génère les contraintes CHECK pour les ENUM avec préservation de casse"""
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    initialize_enum_conversion(cursor)
    enum_columns = collect_enum_columns(cursor)

    print("-- CONTRAINTES CHECK POUR LES TYPES ENUM")
    print()

    for col in enum_columns:
        table_name = col["table"]
        column_name = col["column"]  # ✅ VRAI nom de colonne avec casse préservée
        enum_type = col["enum_type"]

        # Récupérer les valeurs ENUM
        enum_values = get_enum_values(cursor, enum_type)

        if enum_values:
            # Générer la contrainte CHECK
            oracle_type, check_constraint = convert_enum_to_check(
                table_name, column_name, enum_type, enum_values
            )

            if check_constraint:
                # ✅ PRÉSERVER LA CASSE de la table
                safe_table_name = quote_identifier(table_name)
                
                # ✅ PRÉSERVER LA CASSE dans la contrainte CHECK
                check_constraint_quoted = check_constraint.replace(
                    f'"{column_name}"', quote_identifier(column_name)
                )
                
                print(f"ALTER TABLE {safe_table_name} ADD {check_constraint_quoted};")

        print()

    cursor.close()
    conn.close()

def generate_indexes(connection_params):
    """Génère les INDEX avec préservation de casse"""
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)

    tables = [row[0] for row in cursor.fetchall()]

    print("-- INDEX")
    print()

    for table in tables:
        safe_table_name = quote_identifier(table)

        cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = %s AND schemaname = 'public'
                AND indexname NOT LIKE 'pg_toast%%'
        """, (table,))

        indexes = cursor.fetchall()

        for index_name, index_def in indexes:
            # ✅ PRÉSERVER LA CASSE du nom d'index
            safe_index_name = quote_identifier(index_name)

            # Extraire les colonnes de la définition d'index
            match = re.search(r'ON\s+\w+\s+\((.*)\)', index_def, re.IGNORECASE)
            if match:
                columns_str = match.group(1)
                # ✅ PRÉSERVER LA CASSE des colonnes
                columns = [quote_identifier(col.strip()) for col in columns_str.split(',')]
                columns_formatted = ", ".join(columns)

                # Vérifier si c'est un index UNIQUE
                if 'UNIQUE' in index_def.upper():
                    print(f"CREATE UNIQUE INDEX {safe_index_name} ON {safe_table_name} ({columns_formatted});")
                else:
                    print(f"CREATE INDEX {safe_index_name} ON {safe_table_name} ({columns_formatted});")

    print()

    cursor.close()
    conn.close()

def generate_complete_migration(connection_params):
    """Génère la migration complète avec préservation de casse"""
    print()
    print("-- ============================================================================")
    print("-- MIGRATION POSTGRESQL → ORACLE (VERSION 3 - PRESERVE CASE)")
    print("-- ============================================================================")
    print("-- Tous les identifiants sont encadrés de guillemets doubles pour préserver")
    print("-- la casse exacte des noms d'objets source.")
    print("-- ============================================================================")
    print()

    generate_tables(connection_params)
    generate_constraints(connection_params)
    generate_enum_checks(connection_params)
    generate_indexes(connection_params)

    print("-- ============================================================================")
    print("-- FIN DE LA MIGRATION")
    print("-- ============================================================================")

# Configuration exemple
if __name__ == "__main__":
    connection_params = {
        'host': 'localhost',
        'database': 'AURA',
        'user': 'postgres',
        'password': 'admin',
        'port': 5432
    }
    
    generate_complete_migration(connection_params)
