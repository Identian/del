import json
import logging
import sys
import os
import datetime
from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db


#logging.basicConfig() #En lambdas borra este

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event,context):
    sc_obj = script_object(event)
    lambda_response = sc_obj.starter()
    logger.info(f'respuesta final de lambda: \n{lambda_response}')
    return lambda_response


class script_object:
    def __init__(self, event):
        try:
            self.failed_init = False
            logger.info(f'Event que llega:\n{event}')
            self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 
                                               'Access-Control-Allow-Origin': '*', 
                                              'Access-Control-Allow-Methods': '*'}, "statusCode": 500, 'body': {}}
            self.db_connection = 0
            self.detailed_raise = ''
            self.partial_response = list()

            event_dict = event['pathParameters']
            self.id_assessment = event_dict['id-assessment']
            
            self.orchestrator_status = str()
            self.found_notes = list()
    
            logger.warning(f'event de entrada: {str(event)}')

            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.create_conection_to_db()
            self.acquire_orchestrator_status()
            #[TIMEOUT, ERROR, SUCCES, WORKING]
            if self.orchestrator_status in ['SUCCES', 'ERROR']:
                self.acquire_orchestrator_notes()
            if self.delta_time > datetime.timedelta(seconds=30):
                self.acquire_orchestrator_notes()
                self.orchestrator_status = 'TIMEOUT'

            self.organize_partial_response()
                
            return self.response_maker(succesfull_run = True)
            
        except Exception as e:
            logger.error(f'[starter] Hubieron problemas en el comando de la linea: {get_current_error_line()}')
            return self.response_maker(succesfull_run = False, error_str = (str(e)))

    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_db(self):
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)
    
    
    @handler_wrapper('Adquiriendo el status del Engine de recalculo', 'Datos adquiridos con exito', 'Error adquiriendo el status del orquestador', 'Error adquiriendo status')
    def acquire_orchestrator_status(self):
        query = f"""SELECT STATE, LAST_STATE_TS FROM ORCHESTRATOR_STATUS WHERE ID_ASSESSMENT = {self.id_assessment}"""
        logger.info(f'[get_capex_data] Query para obtener status del proceso de valoración:\n{query}')
        rds_data = self.db_connection.execute(query)
        self.orchestrator_status_info = next(item._asdict() for item in rds_data.fetchall())
        self.orchestrator_status = self.orchestrator_status_info['STATE']
        orchestrator_last_timestamp = self.orchestrator_status_info['LAST_STATE_TS']
        now_time = datetime.datetime.now()
        self.delta_time = now_time - orchestrator_last_timestamp
        logger.info(f'[acquire_orchestrator_status] Resultados de status query:\n{self.orchestrator_status_info}')
        logger.info(f'[mira aca] {now_time} - {orchestrator_last_timestamp}')
        logger.warning(f'[deltatime]: {self.delta_time}')

        
    @handler_wrapper('Es posible que hayan notas de orquestador; adquiriendo', 'Adquisición de notas terminado', 'Error adquiriendo anotaciones del orquestador', 'Error chqeuando anotaciones')
    def acquire_orchestrator_notes(self):
        condensed_contexts = ('Pyg', 'Proyecciones pyg')
        query = f"""SELECT TASK, NOTE FROM NOTES WHERE ID_ASSESSMENT = {self.id_assessment}"""
        logger.info(f'[acquire_orchestrator_notes] Query para obtener notas de orquestador para el proceso de valoración:\n{query}')
        
        rds_data = self.db_connection.execute(query)
        found_notes = [item._asdict() for item in rds_data.fetchall()]
        logger.warning(f'[mira aca] found_notes recien salido: {found_notes}')
        contexts_set = list(set(item['TASK'] for item in found_notes if not item['TASK'].startswith(condensed_contexts)))
        contexts_set = sorted(contexts_set + list(condensed_contexts))
        
        self.found_notes = []
        for context in contexts_set:
            logger.info(f'[mira aca] buscando {context} en \n{found_notes}')
            context_notes = [item['NOTE'] for item in found_notes if item['TASK'].startswith(context)]
            if context_notes:
                self.found_notes.append({'context': context, 'notes': context_notes})
        
        
    @handler_wrapper('Organizando respuesta a front', 'Respuesta a front organizada', 'Error organizando respuesta a front', 'Error organizando respuesta')
    def organize_partial_response(self):
        self.partial_response = {'status': self.orchestrator_status}
        if self.found_notes:
            self.partial_response['notes'] = self.found_notes


    @debugger_wrapper('Error construyendo respuesta final', 'Error construyendo respuesta')
    def response_maker(self, succesfull_run = False, error_str = str()):
        if self.db_connection:
            self.db_connection.close()
        if succesfull_run:
            self.final_response['statusCode'] = 200
            self.final_response['body'] = json.dumps(self.partial_response)
            return self.final_response
            
        self.final_response['body'] = json.dumps(self.detailed_raise if self.detailed_raise else error_str)
        return self.final_response



def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    