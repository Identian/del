import base64
import boto3
import json
import logging
import os
import sys
import traceback

from sqlalchemy import create_engine

from decorators import handler_wrapper


logger = logging.getLogger()
logger.setLevel(logging.INFO)


@handler_wrapper('Obteniendo secreto','secreto obtenido','Error al obtener secreto','error fatal de back')
def get_secret(secret_region, secret_name):
    """
    Obtiene las credenciales que vienen del Secret Manager
    :param secret_region: (string) Nombre de la region donde se encuentran las credenciales
    :param secrete_name: (string) Nombre del secreto
    :return: (dict) Diccionario con las credenciales
    """
    session = boto3.session.Session()
    client_secrets_manager = session.client(service_name='secretsmanager', region_name=secret_region)
    secret_data = client_secrets_manager.get_secret_value(SecretId=secret_name)
    if 'SecretString' in secret_data:
        secret_str = secret_data['SecretString']
    else:
        secret_str = base64.b64decode(secret_data['SecretBinary'])
        logger.info('[utils] (get_secret) Se obtuvo el secreto')
    return json.loads(secret_str)


@handler_wrapper('Conectando a base de datos {a}','Conectado correctamente a base de datos','Error en la conexion a base de datos','error fatal de back')
def connect_to_db(db_schema, db_secret):
    """
    Se conecta a dos bases de datos por medio de las variables de entorno
    :return: (tuple) Contiene los objetos sqlalchemy para ejecutar queries
    """

    conn_string = ('mysql+mysqlconnector://' + db_secret["username"] + ':' + db_secret["password"] + '@' + db_secret["host"] + ':' + str(db_secret["port"]) + '/' + db_schema)
    sql_engine = create_engine(conn_string)
    db_connection = sql_engine.connect()
    return db_connection


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)


def get_especific_error_line():
    _, _, exc_tb = sys.exc_info()
    return str(traceback.extract_tb(exc_tb)[-1][1])
