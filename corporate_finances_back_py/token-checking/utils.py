#version 2023 - 05 - 04
import base64
import boto3
import json
import logging

#from sqlalchemy import create_engine

from decorators import handler_wrapper, debugger_wrapper

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@debugger_wrapper('Error al obtener secreto','Error fatal de back conectando a base de datos')
def get_secret(secret_region, secret_name):
    session = boto3.session.Session()
    client_secrets_manager = session.client(service_name='secretsmanager', region_name=secret_region)
    secret_data = client_secrets_manager.get_secret_value(SecretId=secret_name)
    if 'SecretString' in secret_data:
        secret_str = secret_data['SecretString']
    else:
        secret_str = base64.b64decode(secret_data['SecretBinary'])
        logger.info('[utils] (get_secret) Se obtuvo el secreto')
    return json.loads(secret_str)


@debugger_wrapper('Error en la conexion a base de datos','Error fatal de back conectando a base de datos')
def connect_to_db(db_schema, db_secret):
    conn_string = ('mysql+mysqlconnector://' + db_secret["username"] + ':' + db_secret["password"] + '@' + db_secret["host"] + ':' + str(db_secret["port"]) + '/' + db_schema)
    sql_engine = create_engine(conn_string)
    db_connection = sql_engine.connect()
    return db_connection