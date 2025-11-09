"""
Module: type_mapping_fixed.py (VERSION CORRIGÉE)
-------------------------------------------
Conversion des types PostgreSQL vers Oracle avec nettoyage des valeurs DEFAULT.

CORRECTIONS APPORTÉES:
✅ Suppression des cast PostgreSQL (::numeric, ::text, ::character varying)
✅ Conversion de false/true vers 0/1
✅ Nettoyage des valeurs DEFAULT avec syntaxe PostgreSQL
✅ Suppression des références de type ENUM dans les DEFAULT
✅ Gestion des mots réservés Oracle
"""

import re

# Dictionnaire global pour stocker la conversion des ENUM
enum_conversion = {}

def initialize_enum_conversion(cursor):
    """
    Initialise le dictionnaire de conversion des types ENUM depuis PostgreSQL.
    """
    global enum_conversion
    enum_conversion = {}
    
    cursor.execute("""
        SELECT t.typname AS enum_type, e.enumlabel AS enum_value
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        ORDER BY t.typname, e.enumsortorder
    """)
    
    for enum_type, enum_value in cursor.fetchall():
        if enum_type not in enum_conversion:
            enum_conversion[enum_type] = []
        enum_conversion[enum_type].append(enum_value)

def clean_default_value(default_value):
    """
    Nettoie une valeur DEFAULT pour la rendre compatible avec Oracle.
    
    Corrections:
    - Supprime ::numeric, ::text, ::character varying, etc.
    - Convertit false/true en 0/1
    - Supprime les références de type ENUM comme (table.column_enum)
    - Convertit now() en SYSTIMESTAMP
    - Convertit uuid_generate_v4() en SYS_GUID()
    """
    if not default_value:
        return None
    
    # Convertir en chaîne si nécessaire
    default_str = str(default_value).strip()
    
    # Supprimer les cast PostgreSQL (::type)
    default_str = re.sub(r'::[a-zA-Z_]+(\s+varying)?', '', default_str)
    
    # Supprimer les références de type ENUM entre parenthèses
    # Exemple: 'ADMIN'(account.role_enum) → 'ADMIN'
    default_str = re.sub(r'\([a-zA-Z_]+\.[a-zA-Z_]+\)', '', default_str)
    
    # Convertir les booléens
    if default_str.lower() == 'false':
        return '0'
    if default_str.lower() == 'true':
        return '1'
    
    # Convertir les fonctions PostgreSQL
    if 'uuid_generate_v4()' in default_str:
        return 'SYS_GUID()'
    if 'now()' in default_str.lower():
        return 'SYSTIMESTAMP'
    if 'current_timestamp' in default_str.lower():
        return 'SYSTIMESTAMP'
    
    # Supprimer nextval (séquences automatiques)
    if 'nextval(' in default_str.lower():
        return None
    
    # Nettoyer les espaces multiples
    default_str = re.sub(r'\s+', ' ', default_str).strip()
    
    return default_str if default_str else None

def convert_type(pg_type, length=None, precision=None, scale=None):
    """
    Convertit un type PostgreSQL en type Oracle équivalent.
    """
    pg_type_lower = pg_type.lower().strip()
    
    # Types numériques
    if pg_type_lower in ('smallint', 'int2'):
        return 'NUMBER(5)'
    elif pg_type_lower in ('integer', 'int', 'int4'):
        return 'NUMBER(10)'
    elif pg_type_lower in ('bigint', 'int8'):
        return 'NUMBER(19)'
    elif pg_type_lower in ('numeric', 'decimal'):
        if precision and scale:
            return f'NUMBER({precision},{scale})'
        elif precision:
            return f'NUMBER({precision})'
        else:
            return 'NUMBER'
    elif pg_type_lower in ('real', 'float4'):
        return 'BINARY_FLOAT'
    elif pg_type_lower in ('double precision', 'float8', 'float'):
        return 'BINARY_DOUBLE'
    elif pg_type_lower == 'money':
        return 'NUMBER(19,2)'
    
    # Types caractères
    elif pg_type_lower in ('character varying', 'varchar'):
        if length:
            return f'VARCHAR2({length})'
        return 'VARCHAR2(4000)'
    elif pg_type_lower in ('character', 'char', 'bpchar'):
        if length:
            return f'CHAR({length})'
        return 'CHAR(1)'
    elif pg_type_lower == 'text':
        return 'CLOB'
    
    # Types binaires
    elif pg_type_lower == 'bytea':
        return 'BLOB'
    
    # Types date/temps
    elif pg_type_lower == 'date':
        return 'DATE'
    elif pg_type_lower in ('timestamp', 'timestamp without time zone'):
        return 'TIMESTAMP'
    elif pg_type_lower == 'timestamp with time zone':
        return 'TIMESTAMP WITH TIME ZONE'
    elif pg_type_lower == 'time':
        return 'TIMESTAMP'
    elif pg_type_lower == 'time with time zone':
        return 'TIMESTAMP WITH TIME ZONE'
    elif pg_type_lower == 'interval':
        return 'INTERVAL DAY TO SECOND'
    
    # Types booléens
    elif pg_type_lower == 'boolean':
        return 'NUMBER(1)'
    
    # UUID
    elif pg_type_lower == 'uuid':
        return 'VARCHAR2(36)'
    
    # JSON/JSONB
    elif pg_type_lower in ('json', 'jsonb'):
        return 'CLOB'
    
    # Types XML
    elif pg_type_lower == 'xml':
        return 'XMLTYPE'
    
    # Types array (conversion en CLOB)
    elif pg_type_lower.endswith('[]'):
        return 'CLOB'
    
    # Types serial (auto-increment)
    elif pg_type_lower in ('serial', 'serial4'):
        return 'NUMBER(10) GENERATED BY DEFAULT AS IDENTITY'
    elif pg_type_lower == 'bigserial':
        return 'NUMBER(19) GENERATED BY DEFAULT AS IDENTITY'
    elif pg_type_lower == 'smallserial':
        return 'NUMBER(5) GENERATED BY DEFAULT AS IDENTITY'
    
    # Par défaut, retourner VARCHAR2
    else:
        return 'VARCHAR2(4000)'

def get_oracle_reserved_words():
    """
    Retourne la liste des mots réservés Oracle qui nécessitent des guillemets.
    """
    return {
        'access', 'add', 'all', 'alter', 'and', 'any', 'as', 'asc',
        'audit', 'between', 'by', 'char', 'check', 'cluster', 'column',
        'comment', 'compress', 'connect', 'create', 'current', 'date',
        'decimal', 'default', 'delete', 'desc', 'distinct', 'drop',
        'else', 'exclusive', 'exists', 'file', 'float', 'for', 'from',
        'grant', 'group', 'having', 'identified', 'immediate', 'in',
        'increment', 'index', 'initial', 'insert', 'integer', 'intersect',
        'into', 'is', 'level', 'like', 'lock', 'long', 'maxextents',
        'minus', 'mlslabel', 'mode', 'modify', 'noaudit', 'nocompress',
        'not', 'nowait', 'null', 'number', 'of', 'offline', 'on',
        'online', 'option', 'or', 'order', 'pctfree', 'prior', 'privileges',
        'public', 'raw', 'rename', 'resource', 'revoke', 'row', 'rowid',
        'rownum', 'rows', 'select', 'session', 'set', 'share', 'size',
        'smallint', 'start', 'successful', 'synonym', 'sysdate', 'table',
        'then', 'to', 'trigger', 'uid', 'union', 'unique', 'update',
        'user', 'validate', 'values', 'varchar', 'varchar2', 'view',
        'whenever', 'where', 'with'
    }

def quote_identifier_if_needed(identifier):
    """
    Met des guillemets autour d'un identifiant s'il est un mot réservé Oracle.
    """
    reserved_words = get_oracle_reserved_words()
    if identifier.lower() in reserved_words:
        return f'"{identifier.upper()}"'
    return identifier
