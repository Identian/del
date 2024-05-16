import json
import logging
import sys
import os
from datetime import datetime
from sqlalchemy import text
from collections import defaultdict

from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db, connect_to_db_local_dev

logging.basicConfig()
logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(module)s] %(message)s', force=True, datefmt='%Y-%m-%d %H:%M:%S')
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
            self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 
                                               'Access-Control-Allow-Origin': '*', 
                                              'Access-Control-Allow-Methods': '*'}, "statusCode": 500, 'body': {}}
            self.db_connection = 0
            self.detailed_raise = ''
            self.partial_response = list()
    
            logger.warning(f'event de entrada: {str(event)}')

            event_dict = event['pathParameters']
            self.id_assessment = event_dict['id_assessment']
            
            self.capex_data_found = list()            
            self.grouped_capex_name = defaultdict(list)
            
            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.create_conection_to_db()
            self.get_capex_data()
            if not self.capex_data_found:
                return self.response_maker(succesfull_run = True)
            self.organize_response_objects()
            return self.response_maker(succesfull_run = True)
            
        except Exception as e:
            logger.error(f'[starter] Hubieron problemas en el comando de la linea: {get_current_error_line()}')
            return self.response_maker(succesfull_run = False, error_str = (str(e)))

    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_db(self):
        if __name__ == "__main__":
            self.db_connection = connect_to_db_local_dev()
            return
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)


    @handler_wrapper('Buscando datos de capex en bd', 'Datos de capex adquiridos con exito', 'Error adquiriendo datos de capex','Error adquiriendo capex desde bd')    
    def get_capex_data(self):
        query = f"""SELECT A.CAPEX_NAME, A.USED_ACCOUNT_NAME, B.MANUAL_PERCENTAGE, A.PERIODS, A.METHOD
            FROM CAPEX A, CAPEX_VALUES B
            WHERE A.ID_ASSESSMENT = B.ID_ASSESSMENT
            AND A.CAPEX_NAME = B.CAPEX_NAME 
            AND A.ID_ASSESSMENT = {self.id_assessment} ORDER BY B.CALCULATED_DATE"""

        logger.info(f'[get_capex_data] Query para obtener caracteristicas de capex:\n{query}')
        rds_data = self.db_connection.execute(text(query))
        self.capex_data_found = [item._asdict() for item in rds_data.fetchall()]
        logger.info(f'[get_capex_data] Resultados del query de capex:\n{self.capex_data_found}')
        
        if self.capex_data_found:
            for item in self.capex_data_found:
                self.grouped_capex_name[item['CAPEX_NAME']].append(item)
              
    
    @handler_wrapper('Capex antiguo encontrado, organizando respuesta', 'Respuesta de lambda construída', 'Error construyendo respuesta de lambda', 'Error construyendo respuesta')
    def organize_response_objects(self):
        
        for capex in self.grouped_capex_name.values():
            manual_values=list()        
            for row in capex:
                try:
                    manual_values.append(float(row['MANUAL_PERCENTAGE']))
                except:
                    continue            

            self.partial_response.append({'name':capex[0]['CAPEX_NAME'],
                                'method': capex[0]['METHOD'], 
                                'accountProjector': capex[0]['USED_ACCOUNT_NAME'], 
                                'year': capex[0]['PERIODS'], 
                                'manualValues': manual_values})
         
         
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
    

if __name__ == "__main__":
    event = {"pathParameters": {"id_assessment": "2071"}}
    lambda_handler(event, '')

"""AL EJECUTAR, LA SALIDA DEBE TENER LA FORMA:
[
    {
        "name": "capex tab 1",
        "method": Manual",
        "accountProjector": "Ingresos operacionales",
        "year": 5,
        "manualValues": [100000, 200000, 300000, 400000, 500000]
    },
    {
        "name": "capex tab 2",
        "method": "Porcentaje de otra variable",
        "accountProjector": "Ingresos operacionales",
        "year": 5,
        "manualValues": [10, 20, 30, 40, 50]
    }
]"""