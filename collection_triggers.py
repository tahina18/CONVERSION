"""
Module collection_triggers.py
-----------------------------
Ce module contient toutes les fonctions pour la collecte, l'analyse, 
la manipulation et la conversion des triggers PostgreSQL vers Oracle.
"""

import psycopg2
import re


def collect_postgresql_triggers(cursor):
    """
    Récupère tous les triggers PostgreSQL avec leurs détails et affiche les résultats.
    
    :param cursor: curseur psycopg2 connecté à PostgreSQL
    :return: liste des triggers récupérés
    """
    query = """
    SELECT 
        t.trigger_name,
        t.event_manipulation AS trigger_event,
        t.event_object_table AS table_name,
        t.action_timing AS trigger_timing,
        t.action_orientation AS trigger_level,
        t.action_statement AS trigger_action,
        pg_get_functiondef(p.oid) AS function_definition
    FROM information_schema.triggers t
    LEFT JOIN pg_proc p ON p.proname = 
        SUBSTRING(t.action_statement FROM 'EXECUTE (?:FUNCTION|PROCEDURE) ([^(]+)')
    WHERE t.trigger_schema = 'public'
    ORDER BY t.event_object_table, t.trigger_name;
    """
    
    cursor.execute(query)
    triggers = cursor.fetchall()
    
    # Affichage des résultats
    print(f"\n{'='*80}")
    print(f"COLLECTE DES TRIGGERS POSTGRESQL")
    print(f"{'='*80}\n")
    
    if not triggers:
        print("Aucun trigger trouvé dans le schéma 'public'.\n")
        return triggers
    
    print(f"Nombre total de triggers trouvés : {len(triggers)}\n")
    
    for idx, trigger in enumerate(triggers, 1):
        trigger_name, event, table, timing, level, action, func_def = trigger
        
        print(f"--- TRIGGER #{idx} ---")
        print(f"Nom du trigger      : {trigger_name}")
        print(f"Table               : {table}")
        print(f"Événement           : {event}")
        print(f"Timing              : {timing}")
        print(f"Niveau              : {level}")
        print(f"Action              : {action}")
        
        if func_def:
            print(f"\nDéfinition de la fonction :")
            print("-" * 40)
            print(func_def)
            print("-" * 40)
        else:
            print(f"\nDéfinition de la fonction : Non disponible")
        
        print()
    
    print(f"{'='*80}\n")
    
    return triggers


def analyze_triggers(triggers):
    """
    Analyse les triggers et les catégorise en triggers d'auto-incrémentation
    et triggers métier.
    
    :param triggers: liste des triggers récupérés de PostgreSQL
    :return: dictionnaire avec deux catégories de triggers
    """
    auto_increment_triggers = []
    business_triggers = []
    
    for trigger in triggers:
        trigger_name, event, table, timing, level, action, func_def = trigger
        
        # Détection des triggers liés aux séquences (auto-increment)
        if func_def and 'nextval' in func_def.lower():
            auto_increment_triggers.append({
                'name': trigger_name,
                'table': table,
                'type': 'auto-increment',
                'event': event,
                'timing': timing,
                'action': action
            })
        else:
            business_triggers.append({
                'name': trigger_name,
                'table': table,
                'event': event,
                'timing': timing,
                'level': level,
                'action': action,
                'function_def': func_def,
                'type': 'business'
            })
    
    # Affichage de l'analyse
    print(f"\n{'='*80}")
    print(f"ANALYSE DES TRIGGERS")
    print(f"{'='*80}\n")
    
    if auto_increment_triggers:
        print(f"📊 {len(auto_increment_triggers)} trigger(s) d'auto-incrémentation détecté(s):")
        for trg in auto_increment_triggers:
            print(f"   - {trg['name']} sur table {trg['table']}")
        print("   → Ces triggers seront remplacés par GENERATED AS IDENTITY\n")
    
    if business_triggers:
        print(f"📊 {len(business_triggers)} trigger(s) métier détecté(s):")
        for trg in business_triggers:
            print(f"   - {trg['name']} sur table {trg['table']} ({trg['timing']} {trg['event']})")
        print("   → Ces triggers nécessitent une conversion PL/pgSQL → PL/SQL\n")
    
    print(f"{'='*80}\n")
    
    return {
        'auto_increment': auto_increment_triggers,
        'business': business_triggers
    }


def extract_function_body(function_definition):
    """
    Extrait le corps de la fonction depuis la définition complète.
    
    :param function_definition: définition complète de la fonction PL/pgSQL
    :return: corps de la fonction (entre BEGIN et END)
    """
    if not function_definition:
        return "-- Corps de fonction non disponible"
    
    # Recherche du bloc BEGIN...END
    body_start = function_definition.find('BEGIN')
    body_end = function_definition.rfind('END;')
    
    if body_start != -1 and body_end != -1:
        body = function_definition[body_start+5:body_end].strip()
        return body
    
    return function_definition


def convert_plpgsql_to_plsql(plpgsql_code):
    """
    Convertit le code PL/pgSQL en PL/SQL Oracle.
    
    :param plpgsql_code: code PL/pgSQL à convertir
    :return: code PL/SQL Oracle
    """
    if not plpgsql_code:
        return "-- Code non disponible pour conversion"
    
    oracle_code = plpgsql_code
    
    # Conversion des variables contextuelles
    oracle_code = oracle_code.replace("NEW.", ":NEW.")
    oracle_code = oracle_code.replace("OLD.", ":OLD.")
    
    # Conversion des fonctions date/heure
    oracle_code = oracle_code.replace("CURRENT_TIMESTAMP", "SYSTIMESTAMP")
    oracle_code = oracle_code.replace("NOW()", "SYSDATE")
    oracle_code = oracle_code.replace("CURRENT_DATE", "SYSDATE")
    
    # Suppression des RETURN
    oracle_code = oracle_code.replace("RETURN NEW;", "")
    oracle_code = oracle_code.replace("RETURN OLD;", "")
    oracle_code = oracle_code.replace("RETURN NULL;", "")
    
    # Conversion des tests conditionnels sur l'opération
    oracle_code = oracle_code.replace("TG_OP = 'INSERT'", "INSERTING")
    oracle_code = oracle_code.replace("TG_OP = 'UPDATE'", "UPDATING")
    oracle_code = oracle_code.replace("TG_OP = 'DELETE'", "DELETING")
    
    # Conversion des exceptions
    oracle_code = re.sub(
        r"RAISE EXCEPTION '([^']+)'",
        r"RAISE_APPLICATION_ERROR(-20001, '\1')",
        oracle_code
    )
    
    # Conversion des RAISE NOTICE
    oracle_code = re.sub(
        r"RAISE NOTICE '([^']+)'",
        r"DBMS_OUTPUT.PUT_LINE('\1')",
        oracle_code
    )
    
    return oracle_code


def generate_oracle_trigger_ddl(trigger_name, table_name, timing, event, plpgsql_code):
    """
    Génère le DDL Oracle complet pour un trigger.
    
    :param trigger_name: nom du trigger
    :param table_name: nom de la table
    :param timing: BEFORE ou AFTER
    :param event: INSERT, UPDATE, DELETE
    :param plpgsql_code: code PL/pgSQL de la fonction
    :return: DDL Oracle du trigger
    """
    # Extraction du corps de la fonction
    body = extract_function_body(plpgsql_code)
    
    # Conversion en PL/SQL
    oracle_body = convert_plpgsql_to_plsql(body)
    
    # Indentation du code
    oracle_body_lines = oracle_body.split('\n')
    indented_body = '\n'.join(['  ' + line if line.strip() else '' for line in oracle_body_lines])
    
    # Génération du DDL Oracle
    ddl = f"""CREATE OR REPLACE TRIGGER {trigger_name}
{timing} {event} ON {table_name}
FOR EACH ROW
BEGIN
{indented_body}
END;
/
"""
    return ddl


def generate_triggers_ddl(connection_params):
    """
    Fonction principale pour générer les DDL Oracle des triggers.
    Collecte, analyse et convertit tous les triggers PostgreSQL.
    
    :param connection_params: paramètres de connexion PostgreSQL
    """
    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cursor:
            # Étape 1 : Collecter les triggers
            triggers = collect_postgresql_triggers(cursor)
            
            if not triggers:
                print("✓ Aucun trigger à traiter.\n")
                return
            
            # Étape 2 : Analyser et catégoriser
            triggers_data = analyze_triggers(triggers)
            
            # Étape 3 : Gérer les triggers d'auto-incrémentation
            auto_triggers = triggers_data.get('auto_increment', [])
            if auto_triggers:
                print(f"\n{'='*80}")
                print("GESTION DES TRIGGERS D'AUTO-INCRÉMENTATION")
                print(f"{'='*80}\n")
                
                for trg in auto_triggers:
                    print(f"❌ Trigger '{trg['name']}' sur table '{trg['table']}'")
                    print(f"   → NE PAS MIGRER : remplacé par GENERATED AS IDENTITY")
                    print(f"   → La colonne a déjà été convertie\n")
            
            # Étape 4 : Convertir les triggers métier
            business_triggers = triggers_data.get('business', [])
            if business_triggers:
                print(f"\n{'='*80}")
                print("CONVERSION DES TRIGGERS MÉTIER EN ORACLE")
                print(f"{'='*80}\n")
                
                for trg in business_triggers:
                    oracle_ddl = generate_oracle_trigger_ddl(
                        trg['name'],
                        trg['table'],
                        trg['timing'],
                        trg['event'],
                        trg.get('function_def', '')
                    )
                    
                    print(f"-- Trigger PostgreSQL: {trg['name']}")
                    print(f"-- Table: {trg['table']}")
                    print(f"-- Type: {trg['timing']} {trg['event']}")
                    print(f"\n{oracle_ddl}")
                    print("-" * 80 + "\n")
            
            # Résumé final
            print(f"\n{'='*80}")
            print("RÉSUMÉ DE LA MIGRATION DES TRIGGERS")
            print(f"{'='*80}")
            print(f"✓ Triggers auto-incrémentation ignorés : {len(auto_triggers)}")
            print(f"✓ Triggers métier convertis : {len(business_triggers)}")
            print(f"\n⚠ ATTENTION : Vérifiez manuellement chaque trigger converti avant exécution!")
            print(f"{'='*80}\n")


# Exemple d'utilisation
if __name__ == "__main__":
    conn_params = {
        'dbname': 'your_db_name',
        'user': 'your_user',
        'password': 'your_password',
        'host': 'localhost',
        'port': 5432
    }
    
    generate_triggers_ddl(conn_params)
