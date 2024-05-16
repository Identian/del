""":
capas:
capa-pandas-data-transfer v.02

Variables de entorno
ARCHIVES_TABLE : ARCHIVE
COMPANIES_TABLE : COMPANY
DB_SCHEMA : src_corporate_finance
SECRET_DB_NAME : precia/rds8/sources/finanzas_corporativas
SECRET_DB_REGION : us-east-1

RAM: 512 MB

"""


import boto3
import datetime
import json
import logging
import os
import sys
import traceback

from utils import *
import pandas as pd

from decorators import handler_wrapper, timing

################################################
###LAMBDA CANDIDATA A DEPRECIACION POR HU 4530
################################################

logger = logging.getLogger()
logger.setLevel(logging.INFO)
failed_init = False

try:
    logger.info('[__INIT__] Inicializando Lambda ...')
    
    db_schema = os.environ['DB_SCHEMA']
    secret_db_region = os.environ['SECRET_DB_REGION']
    secret_db_name = os.environ['SECRET_DB_NAME']
    companies_table = os.environ['COMPANIES_TABLE']
    archives_table = os.environ['ARCHIVES_TABLE']
    db_secret = get_secret(secret_db_region, secret_db_name)
    logger.info('[__INIT__] Lambda inicializada exitosamente')

except Exception as e:
    failed_init = True
    logger.error(f"[__INIT__] error en inicializacion, motivo: " + str(e))


def lambda_handler(event, context):
    if failed_init:
        response_maker('respuesta de excepcion de lambda handler', succesfull=False, exception='Fallo en la inicializacion')
    
    try:
        logger.info(f'[lambda_handler] el event que me está llegando es: {str(event)}')
        db_connection = connect_to_db(db_schema, db_secret)

        nit = process_event(event)
        company_info = get_nit_info(db_connection, nit)
        companie_archives = get_company_archives(db_connection, company_info)
        
        object_response = companie_archives_cleaner(companie_archives)        
        
        logger.info(f'[Lambda_handler] Tareas de lambda terminadas correctamente')
        return response_maker(object_response , succesfull=True, exception='')
        
    except Exception as e:
        logger.error(f'[Lambda_handler] Tareas de lambda terminadas con errores, linea: {get_current_error_line()} motivo: {str(e)}')
        return response_maker({}, succesfull=False, exception=str(e))


    
@handler_wrapper('Procesando event para obtener el user','Nombre de usuario encontrado', 'Error al procesar event', 'Fallo fatal ')
def process_event(event):
    """:
    Funcion que toma el event que disparó la lambda en busqueda del filename envíado por el front.
    En la transformación del filename se debe anotar: los espacios vacíos del filename se convierten
    en '%20', se debe reemplazar; algunos archivos pueden tener varios puntos en el nombre, por lo
    tanto se debe hacer un split por puntos para eliminar el ultimo y eliminar la extenxión del archivo.
    Se vuelven a unir las partes separadas con puntos y se agrega la extensión json
    :param event: Se puede interpretar como variables base que activan la lambda
    :returns: La funcion debe devolver el filename necesario para la descarga
    """

    nit = event["pathParameters"]["nit"]
    return nit


@handler_wrapper('Buscando id del nit recibido','El id del nit recibido fue encontrado','Error en la busqueda de los datos de la empresa','Error, problemas localizando la informacion de la empresa')
def get_nit_info(db_connection, nit):
    query = f"SELECT * FROM {companies_table} WHERE NIT=\"{nit}\" LIMIT 1"
    rds_data = db_connection.execute(query)
    company_info = dict(rds_data.mappings().all()[0])
    return company_info


@handler_wrapper('Chequeando los archives del nit recibido','Archives encontrados', 'Error chequeando archives', 'error fatal, remitase al administrador')
def get_company_archives(db_connection, company_info):
    query = f"SELECT * FROM {archives_table} WHERE ID_COMPANY={company_info['ID']}"
    logger.info(f"Query a base de datos {query}")

    archives_df= pd.read_sql(query, db_connection);

    logger.warning(archives_df.head(3).to_string())
    return archives_df


@handler_wrapper('Organizando archives por fecha y periodicidad','Dataframe organizado correctamente','Error al hacer sort de los datos','Error fatal, remitase al admin')
def companie_archives_cleaner(archives_df):
    
    archives_df = archives_df.sort_values(['INITIAL_DATE', 'PERIODICITY'])
    logger.warning(archives_df.head(3).to_string())
    
    object_response = {}
    for row in archives_df.itertuples():
        initial_date_str = row[3].strftime("%d-%m-%Y")
        try:
            object_response[initial_date_str].append(row[4])
            continue
        except Exception:
            object_response[initial_date_str] = [row[4]]
    
    return object_response

    
@handler_wrapper('Creando respuesta','Respuesta creada correctamente','Error al crear respuesta','Error fatal al crear respuesta')
def response_maker(object_response , succesfull=False, exception=''):
    """:
    Funcion que construye la respuesta general del lambda, para llegar se activa si todo salió bien en lambda handler
    o si algo salió mal y se disparó el la excepcion general de esta. En caso de llegar a este metodo por medio de una excepcion
    se trae un motivo de falla que no será mostrado al analista a menos que lo esté asesorando un desarrollador.
    :succesfull: determina si el metodo se está ejecutando como excepcion o como salida correcta
    :exception: motivo de error
    :error_line: linea de falla
    :returns: la funcion regresa el objeto alistado que lambda handler rergesará al disparador. status_code dependiente
    si hubieron errores y mensajes acordes o una respuesta positiva con un message ok. En cualquier
    caso se deben agregar tambien los encabezados 'Acces-Control' para que api gateway no corte la comunicacion back-front
    """
    headers = {'headers': {'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*',
                           'Access-Control-Allow-Methods': '*'}}
    if not succesfull:
        try:
            logger.warning('Creando respuesta de error') #cuando es comunicacion lambda a lambda se debe mandar un codigo 200 así haya habido error interno
            error_response = {'statusCode': 200}
            error_response.update(headers)
            error_response['body'] = json.dumps(exception)
            
            logger.warning(f'[response_maker] Respuesta a retornar: {str(error_response)}')
            return error_response
        except Exception as e:
            logger.error(
                f'[response_maker] Error al construir la respuesta de error, linea: {get_current_error_line()}, motivo: {str(e)}')
            return 'Fatal error'

    try:
        logger.info('[response_maker] creando respuesta ')
        ok_response = {'statusCode': 200}
        ok_response.update(headers)

        ok_response["body"] = json.dumps(object_response)
        return ok_response

    except Exception as e:
        logger.error(
            f'[response_maker] Error al construir la respuesta de aceptado, linea: {get_current_error_line()}, motivo: {str(e)}')
        return response_maker({}, succesfull=False, exception=str(e))


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)


def get_especific_error_line():
    _, _, exc_tb = sys.exc_info()
    return str(traceback.extract_tb(exc_tb)[-1][1])