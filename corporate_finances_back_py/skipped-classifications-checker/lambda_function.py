""":
capas:
capa-pandas-data-transfer

variables de entorno:
DB_SCHEMA : src_corporate_finance
RAW_CLASSIFICATION_TABLE : RAW_CLASSIFICATION
SECRET_DB_NAME : precia/rds8/sources/finanzas_corporativas
SECRET_DB_REGION : us-east-1

RAM: 1024 MB

"""

import json
import logging
import sys
import traceback
import time
import copy
from utils import *
import pandas as pd
from decorators import handler_wrapper, timing
import os

#logging.basicConfig() #En lambdas borra este
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    lo = lambda_object(event)
    return lo.starter()
    
    
class lambda_object():

    @handler_wrapper('Obteniendo valores clave de event', 'Valores de event obtenidos correctamente',
               'Error al obtener valores event', 'Fallo en la obtencion de valores clave de event')
    def __init__(self, event) -> None:
        logger.warning(f'event que llega a la lambda: {str(event)}')

        self.raw_classifications_table = os.environ['RAW_CLASSIFICATION_TABLE']

        event_body_json = event["body"]
        event_body_dict = json.loads(event_body_json)
        
        self.data_oriented_index = event_body_dict['data']
        
        self.received_classifications_set = set()
        
        self.raw_classification_data = list()
        self.raw_classification_names = list()

        self.unused_classifications = list()
 
        self.non_alerted_classificacitions = ['No aplica',
                                                'Ingresos operacionales 1',
                                                'Ingresos operacionales 2', 
                                                'Ingresos operacionales 3', 
                                                'Ingresos operacionales 4', 
                                                'Ingresos operacionales 5', 
                                                'Gastos operacionales 1',
                                                'Gastos operacionales 2', 
                                                'Gastos operacionales 3', ] #acá toca ver si el qa quiere omitir cartera
        self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'}}
        

    def starter(self):
        try:
            logger.info(f'[starter] Empezando starter de objeto lambda')
            self.create_conection_to_resources()
            self.get_classification_raw_info()
            self.group_received_classifications()
            self.classification_filter()

            self.db_connection.close()
            return self.response_maker(succesfull = True)
            
        except Exception as e:
            if self.db_connection:
                self.db_connection.close()
            logger.error(f'[starter] Error en el procesamieno del comando de la linea: {get_current_error_line()}, motivo: {e}')
            return self.response_maker(succesfull = False, exception_str = str(e))


    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_resources(self):
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)


    @handler_wrapper('Trayendo todas las clasificaciones en raw_classifications','Clasificaciones obtenidas con exito', 'Error obteniendo clasificaciones raw', 'Error obteniendo clasificaciones raw')
    def get_classification_raw_info(self):
        query = f"SELECT * FROM {self.raw_classifications_table}"
        logger.info(f'[get_classification_raw_info] Query para obtener las clasificaciones raw: {query}')
        query_result = self.db_connection.execute(query).mappings().all()
        raw_classification_data = [dict(item) for item in query_result]
        
        non_parent_items = list(filter(lambda item: item['IS_PARENT'] == 0, raw_classification_data))
        self.non_parent_classification_names = [item['CLASSIFICATION'] for item in non_parent_items]
        
        is_parent_items = list(filter(lambda item: item['IS_PARENT'] == 1, raw_classification_data))
        self.is_parent_classification_names = [item['CLASSIFICATION'] for item in is_parent_items]
        
    
    @handler_wrapper('obteniendo set de clasificaciones recibidas', 'Set de clasificaciones recibidas calculado', 'Error encontrando agrupando las clasificaciones recibidas', 'Error de datos recibidos')
    def group_received_classifications(self):
        self.received_classifications_set = list(set(item['classification'] for item in self.data_oriented_index))
        logger.info(f'[group_received_classifications] set de clasificaciones encontradas:\n {self.received_classifications_set}')


    @handler_wrapper('Inicializando filtrador de clasificaciones', 'Filtrador de clasificaciones terminado con exito', 'Error en el filtrador de clasificaciones', 'Error filtrando informacion')
    def classification_filter(self):
        for classification in self.non_alerted_classificacitions:
            self.non_parent_classification_names.remove(classification)
            
        logger.warning(f'Longitud de non parents antes de filtro: {len(self.non_parent_classification_names)}')
        self.non_parent_classification_names = list(filter(lambda item: item not in self.received_classifications_set, self.non_parent_classification_names))
        logger.warning(f'Longitud de non parents despues de filtro: {len(self.non_parent_classification_names)}')    
        
        logger.warning(f'Longitud de parents antes de filtro: {len(self.is_parent_classification_names)}')
        self.is_parent_classification_names = list(filter(self.check_parents_startwith, self.is_parent_classification_names))
        logger.warning(f'Longitud de parents despues de filtro: {len(self.is_parent_classification_names)}')    
        
        self.skipped_classification = self.non_parent_classification_names + self.is_parent_classification_names 
        
        
    def check_parents_startwith(self, parent):
        for received_classification in self.received_classifications_set:
            if received_classification.startswith(parent):
                return False
        return True
        

    def response_maker(self, succesfull = False, exception_str = str):
        if not succesfull:
            self.final_response['body'] = json.dumps(exception_str)
            return self.final_response
        else:
            self.final_response['body'] = json.dumps(self.skipped_classification)
            return self.final_response



def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)

