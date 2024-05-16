import json
import logging
import sys
import os
import datetime

from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db


#logging.basicConfig() #En lambdas borra este

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    sc_obj = script_object(event)
    lambda_response = sc_obj.starter()
    logger.info(f'respuesta final de lambda: \n{lambda_response}')
    return lambda_response


class script_object:
    def __init__(self, event):
        try:
            self.failed_init = False
            self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 
                                               'Access-Control-Allow-Origin': '*', 
                                              'Access-Control-Allow-Methods': '*'}, "statusCode": 500}
            self.db_connection = 0
            self.detailed_raise = ''
            self.partial_response = {}
            self.empty_good_response = False
    
            logger.warning(f'event de entrada: {str(event)}')
            
            self.company_table = os.environ['COMPANY_TABLE']
            self.archive_table = os.environ['ARCHIVE_TABLE']
            self.assessment_table = os.environ['ASSESSMENT_TABLE']
            self.assessment_steps_table =  os.environ['ASSESSMENT_STEPS_TABLE']
            
            event_dict = event['queryStringParameters']
            self.nit = event_dict['nit']
            self.current_short_date = event_dict['date']
            self.current_long_date = datetime.datetime.strptime(self.current_short_date, "%d-%m-%Y").strftime('%Y-%m-%d %H:%M:%S')
            self.current_periodicity = event_dict['periodicity']
            self.user = event_dict['user']
            self.assessment_data = {}

        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
                
            self.create_conection_to_db()
            self.get_assessment_data()
            self.get_assessment_completed_steps()
                
            self.db_connection.close()
            return self.response_maker(succesfull_run = True)
        except Exception as e:
            if self.db_connection:
                self.db_connection.close()
            logger.error(f'[starter] Hubieron problemas en el comando de la linea: {get_current_error_line()}')
            return self.response_maker(succesfull_run = False, error_str = (str(e)))

    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_db(self):
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)


    @handler_wrapper('Obteniendo ID de proceso de valoracion','Id de proceso de valoracion obtenido exitosamente','Error al encontrar ID de proceso de valoracion','Se encontraron problemas relacionados con el ID del proceso de valoracion')
    def get_assessment_data(self):
        query = f"SELECT * FROM {self.company_table} A, {self.archive_table} B, {self.assessment_table} C WHERE A.ID = B.ID_COMPANY AND C.ID_ARCHIVE = B.ID AND A.NIT = \"{self.nit}\" AND B.INITIAL_DATE = \"{self.current_long_date}\" AND B.PERIODICITY = \"{self.current_periodicity}\" AND C.USER = \"{self.user}\""
        logger.info(f'[get_company_info] Query para obtener datos del proceso de valoracion: {query}')
        rds_data = self.db_connection.execute(query)
        try:
            self.assessment_data = dict(rds_data.one())
            
        except Exception as e:
            self.empty_good_response = True
            self.partial_response = {'data_to_get': [], 'id_assessment': 0}
            logger.warning(f'[mira aca] {str(e)}')
            self.detailed_raise = 'El proceso de valoracion no existe'

        
    @handler_wrapper('Revisando los pasos que se han realizado para el proceso de valoracion', 'Pasos hallados correctamente', 'Error adquiriendo los pasos del proceso de valoracion','Error adquiriendo datos del proceso de valoracion')    
    def get_assessment_completed_steps(self):
        query = f"SELECT SERVICE FROM {self.assessment_steps_table} WHERE ID_ASSESSMENT = {self.assessment_data['ID']}"
        logger.info(f'[get_assessment_completed_steps] Query para obtener los pasos completados del proceso de valoracion: {query}')
        rds_data = self.db_connection.execute(query)
        self.found_steps = [item['SERVICE'] for item in rds_data.all()]
        self.partial_response['data_to_get'] = self.found_steps
        if self.found_steps:
            self.partial_response['id_assessment'] = self.assessment_data['ID']
    
        
    @debugger_wrapper('Error construyendo respuesta final', 'Error construyendo respuesta')
    def response_maker(self, succesfull_run = False, error_str = str()):
        if self.empty_good_response:
            self.empty_good_response = False
            return self.response_maker(succesfull_run = True)
            
        if succesfull_run:
            self.final_response['statusCode'] = 200
            self.final_response['body'] = json.dumps(self.partial_response)
            return self.final_response
            
        self.final_response['body'] = json.dumps(self.detailed_raise if self.detailed_raise else error_str)
        return self.final_response


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)