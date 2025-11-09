import configparser
import psycopg2
import oracledb
from psycopg2 import OperationalError
from oracledb import DatabaseError

from extract_schema_postgres import extract_postgres_schema
from generate_ddl import generate_create_table_ddl
from generate_migration import execute_ddl

def connect_postgresql(conf):
    try:
        conn = psycopg2.connect(
            host=conf['host'],
            port=conf['port'],
            database=conf['database'],
            user=conf['user'],
            password=conf['password']
        )
        print("Connexion PostgreSQL réussie")
        return conn
    except OperationalError as e:
        print(f"Erreur de connexion PostgreSQL: {e}")
        return None

def connect_oracle(conf):
    try:
        dsn_tns = oracledb.makedsn(
            conf['host'],
            conf['port'],
            service_name=conf['service_name']
        )
        conn = oracledb.connect(
            user=conf['user'],
            password=conf['password'],
            dsn=dsn_tns
        )
        print("Connexion Oracle réussie")
        return conn
    except DatabaseError as e:
        print(f"Erreur de connexion Oracle: {e}")
        return None

def main():
    config = configparser.ConfigParser()
    config.read('config.ini')

    pg_conf = config['postgresql']
    ora_conf = config['oracle']

    connpg = connect_postgresql(pg_conf)
    if connpg is None:
        return
    

    connoracle = connect_oracle(ora_conf)
    if connoracle is None:
        return

    # Appeler fonctions de migration ici
    
schema = extract_postgres_schema(connection_params_pg) # type: ignore

ddl_commands = []
for table, meta in schema.items():
    ddl = generate_create_table_ddl(table, meta['columns'], meta['primary_key'])
    ddl_commands.append(ddl)

execute_ddl(connection_params_oracle, ddl_commands) # type: ignore

if __name__ == "__main__":
    main()
