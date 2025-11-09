"""
Module collection_functions_procedures.py (VERSION CORRIGÉE)
-----------------------------------------
Ce module contient toutes les fonctions pour la collecte, l'analyse,
la manipulation et la conversion des fonctions et procédures PostgreSQL vers Oracle.
Intègre la conversion automatique des types via type_mapping.py

✅ CORRECTION APPORTÉE (ligne 27-38):
   - Ajout du filtrage des fonctions uuid-ossp (extensions système PostgreSQL)
   - Ces fonctions ne doivent pas être converties (remplacées par SYS_GUID())
"""

import psycopg2
import re
from type_mapping import convert_type, convert_type_in_context

def collect_postgresql_functions(cursor):
    """
    Récupère toutes les fonctions PostgreSQL avec leurs détails,
    EN EXCLUANT les fonctions de l'extension uuid-ossp.
    
    :param cursor: curseur psycopg2 connecté à PostgreSQL
    :return: liste des fonctions récupérées
    """
    query = """
    SELECT
        n.nspname AS schema_name,
        p.proname AS function_name,
        pg_get_functiondef(p.oid) AS function_definition,
        pg_get_function_result(p.oid) AS return_type,
        pg_get_function_arguments(p.oid) AS arguments,
        CASE
            WHEN p.prokind = 'f' THEN 'FUNCTION'
            WHEN p.prokind = 'p' THEN 'PROCEDURE'
            ELSE 'OTHER'
        END AS function_type
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname = 'public'
      AND p.prokind IN ('f', 'p')
      AND p.proname NOT LIKE 'uuid_%'
      AND p.proname NOT IN (
          'uuid_generate_v1', 'uuid_generate_v1mc',
          'uuid_generate_v3', 'uuid_generate_v4',
          'uuid_generate_v5', 'uuid_nil',
          'uuid_ns_dns', 'uuid_ns_oid',
          'uuid_ns_url', 'uuid_ns_x500'
      )
    ORDER BY function_type, p.proname;
    """
    
    cursor.execute(query)
    functions = cursor.fetchall()
    
    # Affichage des résultats
    print(f"\n{'='*80}")
    print(f"COLLECTE DES FONCTIONS ET PROCÉDURES POSTGRESQL")
    print(f"{'='*80}\n")
    
    if not functions:
        print("Aucune fonction ou procédure métier trouvée dans le schéma 'public'.")
        print("(Les fonctions uuid-ossp système sont automatiquement exclues)\n")
        return functions
    
    print(f"Nombre total de fonctions/procédures trouvées : {len(functions)}\n")
    
    for idx, func in enumerate(functions, 1):
        schema, func_name, definition, return_type, arguments, func_type = func
        print(f"--- {func_type} #{idx} ---")
        print(f"Nom                 : {func_name}")
        print(f"Type                : {func_type}")
        print(f"Arguments           : {arguments if arguments else 'Aucun'}")
        print(f"Type de retour      : {return_type if return_type else 'N/A'}")
        print(f"Définition (extrait):")
        print("-" * 40)
        # Afficher seulement les 300 premiers caractères
        preview = definition[:300] + "..." if len(definition) > 300 else definition
        print(preview)
        print("-" * 40)
        print()
    
    print(f"{'='*80}\n")
    return functions

def analyze_functions_and_procedures(functions):
    """
    Analyse et catégorise les fonctions et procédures.
    
    :param functions: liste des fonctions récupérées
    :return: dictionnaire avec catégories
    """
    pure_functions = []
    trigger_functions = []
    procedures = []
    
    for func in functions:
        schema, func_name, definition, return_type, arguments, func_type = func
        
        # Détection des fonctions trigger (retournent trigger)
        if return_type and 'trigger' in return_type.lower():
            trigger_functions.append({
                'name': func_name,
                'schema': schema,
                'definition': definition,
                'arguments': arguments,
                'return_type': return_type,
                'type': 'trigger_function'
            })
        # Procédures (pas de type de retour)
        elif func_type == 'PROCEDURE':
            procedures.append({
                'name': func_name,
                'schema': schema,
                'definition': definition,
                'arguments': arguments,
                'return_type': return_type,
                'type': 'procedure'
            })
        # Fonctions normales (métier)
        else:
            pure_functions.append({
                'name': func_name,
                'schema': schema,
                'definition': definition,
                'arguments': arguments,
                'return_type': return_type,
                'type': 'function'
            })
    
    # Affichage de l'analyse
    print(f"\n{'='*80}")
    print("ANALYSE DES FONCTIONS ET PROCÉDURES")
    print(f"{'='*80}\n")
    
    if trigger_functions:
        print(f"⚡ {len(trigger_functions)} fonction(s) trigger détectée(s):")
        for func in trigger_functions:
            print(f"   - {func['name']} (retourne: {func['return_type']})")
        print("   → Ces fonctions sont déjà gérées par la conversion des triggers\n")
    
    if pure_functions:
        print(f"📦 {len(pure_functions)} fonction(s) métier détectée(s):")
        for func in pure_functions:
            print(f"   - {func['name']} (retourne: {func['return_type']})")
        print("   → Ces fonctions doivent être converties en PL/SQL\n")
    
    if procedures:
        print(f"🔧 {len(procedures)} procédure(s) détectée(s):")
        for proc in procedures:
            args_preview = proc['arguments'][:50] + "..." if proc['arguments'] and len(proc['arguments']) > 50 else proc['arguments']
            print(f"   - {proc['name']} ({args_preview if args_preview else 'sans arguments'})")
        print("   → Ces procédures doivent être converties en PL/SQL\n")
    
    print(f"{'='*80}\n")
    
    return {
        'trigger_functions': trigger_functions,
        'functions': pure_functions,
        'procedures': procedures
    }

def convert_plpgsql_to_plsql(plpgsql_code):
    """
    Convertit le code PL/pgSQL en PL/SQL Oracle.
    
    :param plpgsql_code: code PL/pgSQL complet
    :return: code PL/SQL Oracle
    """
    oracle_code = plpgsql_code
    
    # Conversion des variables contextuelles (pour fonctions trigger)
    oracle_code = oracle_code.replace("NEW.", ":NEW.")
    oracle_code = oracle_code.replace("OLD.", ":OLD.")
    
    # Conversion des fonctions de date/heure
    oracle_code = oracle_code.replace("CURRENT_TIMESTAMP", "SYSTIMESTAMP")
    oracle_code = oracle_code.replace("NOW()", "SYSDATE")
    oracle_code = oracle_code.replace("CURRENT_DATE", "TRUNC(SYSDATE)")
    
    # Conversion des tests conditionnels sur l'opération (pour triggers)
    oracle_code = oracle_code.replace("TG_OP = 'INSERT'", "INSERTING")
    oracle_code = oracle_code.replace("TG_OP = 'UPDATE'", "UPDATING")
    oracle_code = oracle_code.replace("TG_OP = 'DELETE'", "DELETING")
    
    # Conversion des exceptions
    oracle_code = re.sub(
        r"RAISE EXCEPTION '([^']+)'",
        r"RAISE_APPLICATION_ERROR(-20001, '\1')",
        oracle_code,
        flags=re.IGNORECASE
    )
    
    # Conversion des RAISE NOTICE
    oracle_code = re.sub(
        r"RAISE NOTICE '([^']+)'",
        r"DBMS_OUTPUT.PUT_LINE('\1')",
        oracle_code,
        flags=re.IGNORECASE
    )
    
    # Conversion des fonctions de chaînes
    oracle_code = re.sub(r'\bSUBSTRING\(', 'SUBSTR(', oracle_code, flags=re.IGNORECASE)
    oracle_code = re.sub(r'\bPOSITION\(', 'INSTR(', oracle_code, flags=re.IGNORECASE)
    
    # Conversion COALESCE (déjà compatible, mais on s'assure)
    oracle_code = re.sub(r'\bCOALESCE\(', 'COALESCE(', oracle_code, flags=re.IGNORECASE)
    
    # Suppression des $$ et $function$ délimiteurs PostgreSQL
    oracle_code = re.sub(r'\$\$|\$[a-zA-Z_][a-zA-Z0-9_]*\$', '', oracle_code)
    
    # Conversion PERFORM → SELECT INTO (PostgreSQL specific)
    oracle_code = re.sub(r'\bPERFORM\b', 'SELECT', oracle_code, flags=re.IGNORECASE)
    
    return oracle_code

def parse_and_convert_arguments(arguments):
    """
    Parse et convertit les arguments PostgreSQL en format Oracle.
    
    :param arguments: chaîne d'arguments PostgreSQL (ex: "p_id integer, p_name text")
    :return: chaîne d'arguments Oracle convertis
    """
    if not arguments or arguments.strip() == '':
        return ""
    
    # Diviser les arguments par virgule
    args_list = [arg.strip() for arg in arguments.split(',')]
    converted_args = []
    
    for arg in args_list:
        # Format attendu : "nom_param type" ou "IN/OUT nom_param type"
        parts = arg.split()
        
        if len(parts) >= 2:
            # Gestion des modes IN/OUT/INOUT
            mode = ""
            if parts[0].upper() in ['IN', 'OUT', 'INOUT']:
                mode = parts[0].upper() + " "
                arg_name = parts[1]
                arg_type = ' '.join(parts[2:])
            else:
                arg_name = parts[0]
                arg_type = ' '.join(parts[1:])
            
            # Conversion du type via type_mapping
            oracle_type = convert_type_in_context(arg_type, 'parameter')
            converted_args.append(f"{mode}{arg_name} {oracle_type}")
        else:
            # Si format non reconnu, garder tel quel
            converted_args.append(arg)
    
    return ', '.join(converted_args)

def extract_function_body(function_definition):
    """
    Extrait le corps de la fonction depuis la définition complète PostgreSQL.
    
    :param function_definition: définition complète de la fonction
    :return: corps de la fonction (entre BEGIN et END ou AS $$ ... $$)
    """
    # Pattern 1: AS $$ ... $$ ou AS $body$ ... $body$
    body_pattern = r'AS\s+\$[a-zA-Z0-9_]*\$\s*(.*?)\s*\$[a-zA-Z0-9_]*\$'
    match = re.search(body_pattern, function_definition, re.DOTALL | re.IGNORECASE)
    
    if match:
        body = match.group(1).strip()
        return body
    
    # Pattern 2: BEGIN ... END direct
    begin_pattern = r'BEGIN\s+(.*?)\s+END;'
    match = re.search(begin_pattern, function_definition, re.DOTALL | re.IGNORECASE)
    
    if match:
        return "BEGIN\n" + match.group(1).strip() + "\nEND;"
    
    # Si rien ne matche, retourner la définition entière nettoyée
    return function_definition.strip()

def generate_oracle_function_ddl(func_name, arguments, return_type, plpgsql_definition, is_procedure=False):
    """
    Génère le DDL Oracle pour une fonction ou procédure PostgreSQL.
    
    :param func_name: nom de la fonction/procédure
    :param arguments: arguments PostgreSQL
    :param return_type: type de retour PostgreSQL (None pour procédure)
    :param plpgsql_definition: définition PL/pgSQL complète
    :param is_procedure: True si c'est une procédure
    :return: DDL Oracle complet
    """
    # Extraction du corps de la fonction
    body = extract_function_body(plpgsql_definition)
    
    # Conversion du corps en PL/SQL
    oracle_body = convert_plpgsql_to_plsql(body)
    
    # Conversion des arguments
    oracle_arguments = parse_and_convert_arguments(arguments)
    
    # Génération de la signature
    if is_procedure:
        signature = f"CREATE OR REPLACE PROCEDURE {func_name}"
        if oracle_arguments:
            signature += f"(\n  {oracle_arguments}\n)"
    else:
        signature = f"CREATE OR REPLACE FUNCTION {func_name}"
        if oracle_arguments:
            signature += f"(\n  {oracle_arguments}\n)"
        
        # Conversion du type de retour
        if return_type:
            oracle_return = convert_type_in_context(return_type, 'return')
            signature += f"\nRETURN {oracle_return}"
    
    # Génération du DDL complet
    ddl = signature + "\n"
    ddl += "IS\n"
    
    # Indentation du corps
    body_lines = oracle_body.split('\n')
    indented_body = '\n'.join(['  ' + line if line.strip() else '' for line in body_lines])
    ddl += indented_body
    
    # S'assurer qu'il y a un END; à la fin
    if not oracle_body.strip().endswith('END;'):
        ddl += "\nEND;\n"
    
    ddl += "/\n"
    
    return ddl

def generate_functions_procedures_ddl(connection_params):
    """
    Fonction principale pour générer les DDL Oracle des fonctions et procédures.
    Collecte, analyse et convertit toutes les fonctions/procédures PostgreSQL
    (EN EXCLUANT les fonctions uuid-ossp système).
    
    :param connection_params: paramètres de connexion PostgreSQL
    """
    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cursor:
            # Étape 1 : Collecter les fonctions et procédures
            functions = collect_postgresql_functions(cursor)
            
            if not functions:
                print("✓ Aucune fonction ou procédure métier à traiter.\n")
                return
            
            # Étape 2 : Analyser et catégoriser
            data = analyze_functions_and_procedures(functions)
            
            # Étape 3 : Gérer les fonctions trigger (déjà traitées avec les triggers)
            trigger_functions = data.get('trigger_functions', [])
            if trigger_functions:
                print(f"\n{'='*80}")
                print("FONCTIONS TRIGGER")
                print(f"{'='*80}\n")
                for func in trigger_functions:
                    print(f"⚠ Fonction trigger '{func['name']}'")
                    print(f"   → Déjà gérée par la conversion des triggers (ne pas créer séparément)\n")
            
            # Étape 4 : Convertir les fonctions métier
            pure_functions = data.get('functions', [])
            if pure_functions:
                print(f"\n{'='*80}")
                print("CONVERSION DES FONCTIONS EN ORACLE")
                print(f"{'='*80}\n")
                
                for func in pure_functions:
                    oracle_ddl = generate_oracle_function_ddl(
                        func['name'],
                        func['arguments'],
                        func['return_type'],
                        func['definition'],
                        is_procedure=False
                    )
                    
                    print(f"-- Fonction PostgreSQL: {func['name']}")
                    print(f"-- Arguments: {func['arguments'] if func['arguments'] else 'Aucun'}")
                    print(f"-- Type de retour: {func['return_type']}")
                    print(f"\n{oracle_ddl}")
                    print("-" * 80 + "\n")
            
            # Étape 5 : Convertir les procédures
            procedures = data.get('procedures', [])
            if procedures:
                print(f"\n{'='*80}")
                print("CONVERSION DES PROCÉDURES EN ORACLE")
                print(f"{'='*80}\n")
                
                for proc in procedures:
                    oracle_ddl = generate_oracle_function_ddl(
                        proc['name'],
                        proc['arguments'],
                        None,
                        proc['definition'],
                        is_procedure=True
                    )
                    
                    print(f"-- Procédure PostgreSQL: {proc['name']}")
                    print(f"-- Arguments: {proc['arguments'] if proc['arguments'] else 'Aucun'}")
                    print(f"\n{oracle_ddl}")
                    print("-" * 80 + "\n")
            
            # Résumé final
            print(f"\n{'='*80}")
            print("RÉSUMÉ DE LA MIGRATION DES FONCTIONS/PROCÉDURES")
            print(f"{'='*80}")
            print(f"⚡ Fonctions trigger ignorées : {len(trigger_functions)}")
            print(f"✓ Fonctions métier converties : {len(pure_functions)}")
            print(f"✓ Procédures converties : {len(procedures)}")
            print(f"\n⚠ ATTENTION :")
            print(f"   - Vérifiez manuellement chaque fonction/procédure convertie")
            print(f"   - Testez la logique métier en Oracle avant déploiement")
            print(f"   - Les conversions complexes peuvent nécessiter des ajustements manuels")
            print(f"   - Vérifiez les types de paramètres et de retour")
            print(f"   - Les fonctions uuid-ossp sont automatiquement exclues")
            print(f"{'='*80}\n")
