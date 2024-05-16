""":
=============================================================

Nombre: lbd-dev-layer-fincor-companies-create-post
Tipo: Lambda AWS
Autor: Jeisson David Botache Yela
Tecnología - Precia

Ultima modificación: 04/08/2022

Para el proyecto Delta de finanzas corporativas se requiere que se realice una clasificacion
'default' de las cuentas, esta clasificacion es dependiente del tipo de empresa que haya
elegido el analista,


Requerimientos:
capa-pandas-data-transfer v5


variables de entorno:
DB_SCHEMA : src_corporate_finance
SECRET_DB_NAME : precia/rds8/sources/finanzas_corporativas
SECRET_DB_REGION : us-east-1
TABLE_NAME : COMPANY
=============================================================
"""

import boto3
import json
import logging
import sys
import traceback


from utils import *
from sqlalchemy import create_engine
from decorators import handler_wrapper

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
    logger.error(f"event de entrada: \n{str(event)}") 
    if failed_init:
        return response_maker()
    try:
        key_values = process_event(event)
        company_to_db(key_values)

        logger.info('[Lambda Handler] Tareas de la lambda terminadas satisfactoriamente, creando respuesta...')
        return response_maker(succesfull=True, exception='')

    except Exception as e:
        logger.error(
            f'[Lambda Handler] Tareas de la lambda reportan errores fatales en el comando de la linea: {get_current_error_line()}, motivo: {str(e)}, creando respuesta...')
        return response_maker(succesfull=False, exception=str(e))


@handler_wrapper('Obteniendo valores clave de event', 'Valores de event obtenidos correctamente',
               'Error al obtener valores event', 'Fallo en la obtencion de valores clave de event')
def process_event(event):
    event_body_json = event["body"]
    event_body_dict = json.loads(event_body_json)
    key_values = {}
    key_values['Nit'] = event_body_dict["nit"].strip()
    key_values['Name'] = event_body_dict["name"]
    key_values['Sector'] = event_body_dict['sector']

    return key_values


@handler_wrapper('Cargando empresa a base de datos', 'Empresa agregada a base de datos satisfactoriamente',
               'Error al cargar empresa a base de datos', 'Fallo en agregar la empresa a base de datos, es posible que la empresa ya exista')
def company_to_db(key_values):
 
    db_connection = connect_to_db(db_schema, db_secret)
    
    query = f"INSERT INTO {table_name} (NIT, SECTOR ,NAME) VALUES (\"{key_values['Nit']}\", \"{key_values['Sector']}\", \"{key_values['Name']}\")"

    logger.info(f"query a base de datos crear empresa {query} ") #TODO: borrar esto en paso a pruebas
    db_connection.execute(query)


def response_maker(succesfull = False , exception = ''):
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
            'body' : json.dumps(f'error de ejecución interno, motivo: {exception}')}
            error_response.update(headers)
            return error_response
        except Exception as e:
            logger.error(f'[response_maker] Error al construir la respuesta de error, linea: {get_current_error_line()}, motivo: {str(e)}')
            return 'Fatal error'
           
           
    try:

        ok_response  = {'statusCode': 200}
        ok_response.update(headers)
        body = {'message':'Empresa creada satisfactoriamente'}
        ok_response["body"] = json.dumps(body)
        return ok_response
    
    except Exception as e:
        logger.error(f'[response_maker] Error al construir la respuesta de terminacion satisfactoria, linea: {get_current_error_line()}, motivo: {str(e)}')
        return response_maker(succesfull = False, exception = str(e))
        
        
        
def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    
def get_especific_error_line():
    _, _, exc_tb = sys.exc_info()
    return str(traceback.extract_tb(exc_tb)[-1][1])