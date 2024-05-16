""":
capas
capa-pandas-data-transfer

variables de entorno
DB_SCHEMA : src_corporate_finance
RAW_CLASSIFICATION_TABLE : RAW_CLASSIFICATION
SECRET_DB_NAME : precia/rds8/sources/finanzas_corporativas
SECRET_DB_REGION : us-east-1

RAM: 512 MB
"""


import boto3
import datetime
import json
import logging
import sys
import traceback
import os
from sqlalchemy import text


from utils import *
from decorators import handler_wrapper, timing


def lambda_handler(event, context):
    logger.info(f'[lambda_handler] Event de entrada: {event}\nContext de entrada: {context}')
    sc_obj = script_object()
    return sc_obj.starter()
    
    
class script_object:
    def __init__(self):
        try:
            logger.info('[__INIT__] Inicializando objeto lambda ...')
            self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'}, 'statusCode': 200}
            self.db_connection = ''

            self.failed_init = False
            logger.info('[__INIT__] Objeto lambda inicializada exitosamente')
            
        except Exception as e:
            self.failed_init = True
            logger.error(f"[__INIT__] error en inicializacion, linea: {get_especific_error_line()}, motivo: "+str(e)) 


    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Falla inicializacion, revisar logs')
            self.create_clients()
            self.get_classifications()
            
            self.db_connection.close()
            return self.response_maker(succesfull = True)
            
        except Exception as e:
            if self.db_connection:
                self.db_connection.close()
            logger.error(f'[starter] Error en el procesamieno del comando de la linea: {get_current_error_line()}, motivo: {e}')
            return self.response_maker(succesfull = False, exception_str = str(e))

    @handler_wrapper('Creando clientes a servicios externos','Clientes a servicios construidos','Error construyendo conexiones a recursos externos','Problemas requiriendo recursos externos')
    def create_clients(self):
        if __name__ != 'lambda_function':
            self.db_connection = connect_to_db_local_dev()
            print('print de debug')
            self.db_connection.begin()
            print('print de debug')
            return
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)


    @handler_wrapper('Obteniendo listado de clasificaciones','Listado obtenido','Error obteniendo listado de clasificaciones','Error obteniendo listado de clasificaciones')
    def get_classifications(self):
        query = "SELECT CLASSIFICATION FROM RAW_CLASSIFICATION WHERE IS_PARENT = 0"
        logger.info(f"Query a base de datos para obtener la data del puc:\n {query}")  
        rds_data = self.db_connection.execute(query)
        self.classifications_array = list(map(lambda item: item['CLASSIFICATION'],  rds_data.mappings().all()  ))
        logger.warning(f'array obtenido: {self.classifications_array} de tipo {type(self.classifications_array)}')

 

    def response_maker(self, succesfull = False, exception_str = str()):
        if not succesfull:
            self.final_response['body'] = json.dumps(exception_str)
            return self.final_response
        self.final_response['body'] = json.dumps(self.classifications_array)
        return self.final_response
        
        
        
        
def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    
    
def get_especific_error_line():
    _, _, exc_tb = sys.exc_info()
    return str(traceback.extract_tb(exc_tb)[-1][1])
    
    
if __name__ == '__main__':
    lambda_handler('','')