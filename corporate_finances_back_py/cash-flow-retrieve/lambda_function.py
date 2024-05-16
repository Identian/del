

import json
from sqlalchemy import text
import logging
import sys
import os
import math

from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db, connect_to_db_local_dev
from vars import cash_flow_all_items

logging.basicConfig()
logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(module)s] %(message)s', force=True, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    sc_obj = script_object(event)
    lambda_response = sc_obj.starter()
    logger.info(f'respuesta final de lambda: \n{lambda_response}')
    return lambda_response
    
    
class script_object():

    @handler_wrapper('Obteniendo valores clave de event', 'Valores de event obtenidos correctamente',
               'Error al obtener valores event', 'Fallo en la obtencion de valores clave de event')
    def __init__(self, event) -> None:
        try:
            self.failed_init = False
            self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 
                                               'Access-Control-Allow-Origin': '*', 
                                              'Access-Control-Allow-Methods': '*'}, "statusCode": 500, 'body': {}}
            self.db_connection = 0
            self.detailed_raise = ''
            self.partial_response = list()
    
            logger.warning(f'event de entrada: {str(event)}')
            event_dict = event['pathParameters']
            self.id_assessment = event_dict['id_assessment']
            
            self.historic_dates = list()
            self.historic_dates_len = int()
            self.projection_dates = list()
            self.projection_dates_len = int()
            self.total_asssessment_dates = int()
            self.cash_flow_table = dict()
            self.pyg_vector = dict()
            self.debt_data = False
            self.capex_exists = False
            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True
        
    
    def starter(self):
        try:
            logger.info('[starter] Empezando starter de objeto lambda')
            self.create_conection_to_resources()
            self.get_assessment_dates()
            self.initialize_zero_vectors()
            self.acquire_cash_flow_results()
            self.consume_cash_flow()
            self.organize_final_response()
            
            logger.info('[starter] Tareas de starter terminadas con exito')
            return self.response_maker(succesfull_run = True)
            
        except Exception as e:
            logger.error(f'[starter] Error en el procesamieno del comando de la linea: {get_current_error_line()}, motivo: {e}')
            return self.response_maker(succesfull_run = False, error_str = str(e))



    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_resources(self):
        if __name__ == "__main__":
            self.db_connection = connect_to_db_local_dev()
            return
        
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)


    @handler_wrapper('Obteniendo las fechas del proceso de valoración', 'Fechas del proceso de valroación obtenidas con exito', 'Error obteniendo las fechas del proceso de valoración', 'Error obteniendo fechas del proceso de valoración')
    def get_assessment_dates(self):
        query = "SELECT DATES, PROPERTY FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = :id_assessment ORDER BY DATES"
        logger.info(f"[get_assessment_dates] Query a base de datos para obtener las fechas utilizadas en el proces de valoración:\n {query}")
        rds_data = self.db_connection.execute(text(query), {"id_assessment": self.id_assessment})
        directory = {'HISTORIC': self.historic_dates, 'PROJECTION': self.projection_dates}
        for date_item in rds_data.fetchall():
            directory.get(date_item.PROPERTY, []).append(date_item.DATES)
            self.total_asssessment_dates = self.total_asssessment_dates +1

        self.historic_dates = [date.strftime('%d-%m-%Y') for date in self.historic_dates]
        self.projection_dates = [date.strftime('%Y') for date in self.projection_dates]
        self.projection_dates[0] = self.projection_dates[0] if '-12-' in self.historic_dates[-1] else f'Diciembre {self.projection_dates[0]}' #Se agrega esta linea para que llegue diciembre con formato de anualización a front

        self.historic_dates_len = len(self.historic_dates)
        self.projection_dates_len = len(self.projection_dates)
        self.total_dates_len = self.historic_dates_len + self.projection_dates_len

        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')
    
    
    @handler_wrapper('Inicializando todos los vectores a zero', 'Vectores de flujo de caja inicializados', 'Error inicializando vectores de flujo de caja', 'Error iniciando sistema vectorial')
    def initialize_zero_vectors(self):
        for row in cash_flow_all_items:
            self.cash_flow_table[row] = [0] * self.total_asssessment_dates
            
    
    
    @handler_wrapper('Adquiriendo resultados de tabla de flujo de caja', 'Datos de flujo de caja adquiridos con éxito', 'Error adquiriendo resultados de tabla de flujo de caja', 'Error adquiriendo resultados')
    def acquire_cash_flow_results(self):
        query = "SELECT A.SUMMARY_DATE, A.VALUE AS value, B.CASH_FLOW_ITEM_NAME FROM CASH_FLOW A, RAW_CASH_FLOW B WHERE A.ID_RAW_CASH_FLOW = B.ID AND A.ID_ASSESSMENT = :id_assessment ORDER BY A.SUMMARY_DATE"
        logger.info(f"[acquire_cash_flow_results] Query a base de datos para obtener los resultados de engine para el flujo de caja:\n{query}")
        rds_data = self.db_connection.execute(text(query), {'id_assessment':self.id_assessment})
        self.acquired_cash_flow_results = [row._asdict() for row in rds_data.fetchall()]
        for row in self.acquired_cash_flow_results:
            row['value'] = float(row['value'])


    @handler_wrapper('Consumiendo datos adquiridos de flujo de caja', 'Datos adquiridos consumidos con éxito', 'Error consumiendo datos de flujo de caja', 'Error construyendo tabla de flujo de caja')
    def consume_cash_flow(self):
        for key in self.cash_flow_table:
            key_found_vector = [row['value'] for row in self.acquired_cash_flow_results if row['CASH_FLOW_ITEM_NAME'] == key]
            if len(key_found_vector) != self.total_dates_len:
                logger.warning(f'[] El vector de {key} tiene longitud diferente a la cantidad de fechas\nFechas:{self.total_dates_len}\nVector: {key_found_vector}')
            self.cash_flow_table[key] = key_found_vector
            if key == 'Check':
                self.cash_flow_table[key] = ['Sí'] * self.total_asssessment_dates


    @handler_wrapper('Organizando respuesta final', 'Respuesta final organizada satisfactoriamente', 'Error organizando respuesta final', 'Error organizando respuesta de servicio')
    def organize_final_response(self):
        data = []
        
        self.cash_flow_table['Otros ingresos y egresos no operativos CF'] = self.cash_flow_table['Otros ingresos y egresos no operativos']
        for key, vector in self.cash_flow_table.items():
            data.append({'name': key, 'values': {'history': vector[:len(self.historic_dates)], 'projection': vector[-1 * len(self.projection_dates):]}})
            

        self.partial_response = {'datesHistory': self.historic_dates, 'datesProjection': self.projection_dates, 'data': data}
        
        
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

    
def checker(a,b):
    if math.isclose(a, b, rel_tol=0.001, abs_tol=0.1):
        return 'Sí'
    return 'No'

def safe_exit(j):
    try:
        return float(j)
    except:
        return 0
    

if __name__ == "__main__":
    event = {"pathParameters": {"id_assessment": "51"}}
    lambda_handler(event, '')