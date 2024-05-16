""":
=============================================================

Nombre: lbd-dev-layer-fincor-companies-exists-get
Tipo: Lambda AWS
Autor: Jeisson David Botache Yela
Tecnología - Precia

Ultima modificación: 03/08/2022

Para el proyecto Delta de finanzas corporativas se requiere que se realice una clasificacion
'default' de las cuentas


Requerimientos:
capa-pandas-data-transfer v.02


variables de entorno:
DB_SCHEMA : src_corporate_finance
SECRET_DB_NAME : precia/rds8/sources/finanzas_corporativas
SECRET_DB_REGION : us-east-1
TABLE_NAME : COMPANY

RAM: 512 MB
=============================================================
"""

import boto3
import json
import logging
import sys
import traceback

from utils import *

from decorators import handler_wrapper, timing

logger = logging.getLogger()
logger.setLevel(logging.INFO)
failed_init = False

try:
    logger.info('[__INIT__] Inicializando Lambda ...')
    db_schema = os.environ['DB_SCHEMA']
    secret_db_name = os.environ['SECRET_DB_NAME']
    secret_db_region = os.environ['SECRET_DB_REGION']
    table_name = os.environ['TABLE_NAME']
    s3_client = boto3.client('s3')

    db_secret = get_secret(secret_db_region, secret_db_name)
    logger.info('[__INIT__] Lambda inicializada exitosamente')


except Exception as e:
    failed_init = True
    logger.error(f"[__INIT__] error en inicializacion, motivo: " + str(e))


def lambda_handler(event, context):
    #TODO: documentar metodo
    if failed_init:
        return {'statusCode': 500, 'body': json.dumps('Error al inicializar lambda')}

    try:
        db_connection = connect_to_db(db_schema, db_secret)
        key_values = process_event(event)
        companie_exists = search_existing(key_values, db_connection)
        
        logger.info('[Lambda Handler] Tareas de la lambda terminadas satisfactoriamente, creando respuesta...')
        return response_maker(companie_exists, succesfull=True, exception='')

    except Exception as e:
        logger.error(
            f'[Lambda Handler] Tareas de la lambda reportan errores fatales en el comando de la linea: {get_current_error_line()}, motivo: {str(e)}, creando respuesta...')
        response_maker(True,succesfull=False, exception=str(e))

@handler_wrapper('Buscando nit en event','Nit encontrado satisfactoriamente','Error al buscar nit en event','No se pudo encontrar el nit en la dirección requerida')
def process_event(event):
    #TODO: documentar metodo
    logger.info(f'{str(event)}') #TODO: borra esto en paso a pruebas
    key_values = {}
    key_values['Nit'] = event["pathParameters"]["nit"]
    logger.info(f"nit encontrado: {key_values['Nit']}")

    return key_values

@handler_wrapper('Buscando nit en base de datos','Busqueda terminada','Error al buscar nit en base de datos','Error al buscar, reporte al administrador del sistema')
def search_existing(key_values, db_connection):
    #TODO: documentar metodo
    query = f"SELECT * FROM {table_name} WHERE NIT=\"{key_values['Nit']}\" LIMIT 1"
    logger.info(f'Query a base de datos: {query}')
    rds_data = db_connection.execute(query)
    

    if rds_data.rowcount != 0:
        resultado = dict(rds_data.mappings().all()[0])

        logger.info(f'{resultado}')     #TODO: borra esto en paso a pruebas
        logger.info(f'{type(resultado)}') #TODO: borra esto en paso a pruebas
        return resultado
    return {}
    
def response_maker(companie_exists, succesfull = False , exception = ''):
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
    headers =  {'headers': {'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*','Access-Control-Allow-Methods': '*'}}
    if not succesfull:
        try:
            error_response  = {'statusCode': 500,
            'body' : json.dumps(f'error de ejecución, motivo: {exception}')}
            error_response.update(headers)
            return error_response
        except Exception as e:
            logger.error(f'[response_maker] Error al construir la respuesta de error, linea: {get_current_error_line()}, motivo: {str(e)}')
            return 'Fatal error'
           
        
    try:

        ok_response  = {'statusCode': 200}
        ok_response.update(headers)
        body = {}
        ok_response["body"] = json.dumps(companie_exists) #TODO:revisar esto con Daniel, objeto o json
        return ok_response
    
    except Exception as e:
        logger.error(f'[response_maker] Error al construir la respuesta de terminacion satisfactoria, linea: {get_current_error_line()}, motivo: {str(e)}')
        return response_maker({}, succesfull = False, exception = str(e))
        
        
def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    
def get_especific_error_line():
    _, _, exc_tb = sys.exc_info()
    return str(traceback.extract_tb(exc_tb)[-1][1])