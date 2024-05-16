from sqlalchemy import text
import datetime
import json
import logging
import os
import sys
import sqlalchemy
import traceback

if __name__ in ['__main__', 'lambda_function']:
    from decorators import handler_wrapper, timing, debugger_wrapper
    from utils import get_secret, connect_to_db, connect_to_db_local_dev

else:
    from .decorators import handler_wrapper, timing, debugger_wrapper
    from .utils import get_secret, connect_to_db, connect_to_db_local_dev

logging.basicConfig()
#logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(module)s] %(message)s', force=True, datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', force=True, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    sc_obj = script_object(event)
    lambda_response = sc_obj.starter()
    logger.info(f'respuesta final de lambda: \n{lambda_response}')
    return lambda_response
    
    
class script_object():
    def __init__(self, event) -> None:
        try: 
            self.failed_init = False
            self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 
                                               'Access-Control-Allow-Origin': '*', 
                                              'Access-Control-Allow-Methods': '*'}, "statusCode": 500, 'body': {}}
            self.db_connection = 0
            self.detailed_raise = ''
            self.partial_response = {}
            self.id_assessment = 0
    
            logger.warning(f'event de entrada: {str(event)}')
            event_dict = event.get('queryStringParameters', False)
            self.nit = event_dict['nit']
            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True


    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
                
            logger.info('[starter] Empezando starter de objeto lambda')
            self.create_conection_to_db()
            self.adquire_company_info()
            self.check_model_existance()
            self.adquire_archives_info()
            self.adquire_assessment_info()
            self.merge_assessment_data()
            

            return self.response_maker(succesfull_run = True)
        except Exception as e:
            logger.error(f'[starter] Hubieron problemas en el comando de la linea: {get_current_error_line()}')
            return self.response_maker(succesfull_run = False, error_str = (str(e)))



    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_db(self):
        if __name__ != 'lambda_function':
            self.db_connection = connect_to_db_local_dev()
            return
        
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)
        
        
    @handler_wrapper('Adquiriendo información de la empresa', 'Información de empresa adquirida con exito', 'Error adquiriendo informacion de la empresa', 'Error adquiriendo información de la empresa')
    def adquire_company_info(self):
        query = f"SELECT * FROM COMPANY WHERE NIT = '{self.nit}'"    
        logger.info(f"[get_raw_pyg] Query a base de datos para obtener la información de la empresa:\n{query}")
        rds_data = self.db_connection.execute(text(query))
        self.company_info = [row._asdict() for row in rds_data.fetchall()]
        self.partial_response['company'] = {'name': self.company_info[0]['NAME'], 'sector': self.company_info[0]['SECTOR']}
        
    @handler_wrapper('Chqueando si la empresa tiene modelo', 'Chequeo de modelo terminado', 'Error chequeando si la empresa tiene modelo', 'Error chequeando modelo')
    def check_model_existance(self):
        query = f"SELECT EXISTS (SELECT * FROM MODEL_USER_CLASSIFICATION WHERE ID_COMPANY = {self.company_info[0]['ID']})"    
        logger.info(f"[check_model_existance] Query a base de datos para chequear si la empresa tiene modelo guardado :\n{query}")
        rds_data = self.db_connection.execute(text(query))
        model_exists = rds_data.scalar()
        logger.info(f'[check_model_existance] Bool de modelo existe:\n{model_exists}')
        if model_exists:
            self.partial_response['model'] = 'Sí'
    
        
    @handler_wrapper('Adquiriendo infromación de los procesos de la empresa', 'Información de pucs adquirida con exito', 'Error adquiriendo informacion de los puc de la empresa', 'Error adquiriendo información de pucs|')
    def adquire_archives_info(self):
        query = f"SELECT ID, INITIAL_DATE, PERIODICITY AS periodicity FROM ARCHIVE WHERE ID_COMPANY = {self.company_info[0]['ID']} ORDER BY INITIAL_DATE"    
        logger.info(f"[adquire_archives_info] Query a base de datos para obtener la información de los pucs de la empresa:\n{query}")
        rds_data = self.db_connection.execute(text(query))
        self.archives_info = [row._asdict() for row in rds_data.fetchall()]
        for row in self.archives_info:
            row['date'] = row['INITIAL_DATE'].strftime('%d-%m-%Y')
            del row['INITIAL_DATE']
        logger.info(f'[adquire_archives_info] Archives de la empresa encontrados:\n{self.archives_info}')
        
        
    @handler_wrapper('Adquiriendo información de assessments', 'Información de assessments adquirida con exito', 'Error adquiriendo informacion de assessments', 'Error adquiriendo información de procesos de valoracion')
    def adquire_assessment_info(self):
        found_archives = [archive['ID'] for archive in self.archives_info]
        archives_str = str(found_archives).replace('[','').replace(']','')
        query = f"SELECT * FROM ASSESSMENT WHERE ID_ARCHIVE in ({archives_str})"
        logger.info(f"[adquire_assessment_info] Query a base de datos para obtener la información de los procesos de valoración creados para esta emrpesa:\n{query}")
        rds_data = self.db_connection.execute(text(query))
        self.assessment_records = [row._asdict() for row in rds_data.fetchall()]
        
    
    @handler_wrapper('Emergiendo los assessments encontrados con los archives de la empresa', 'Datos adquiridos unidos correctamente', 'Error emergiendo datos adquiridos', 'Error al relacionar procesos de valoración y archives de puc')
    def merge_assessment_data(self):
        response_data = []
        for row in self.archives_info:
            assessments_created = [item for item in self.assessment_records if item['ID_ARCHIVE'] == row['ID']]
            if not assessments_created:
                response_data.append(row)

            for found_assessment in assessments_created:
                row['user'] = found_assessment['USER']
                row['id_assessment'] = found_assessment['ID']
                response_data.append(row.copy())


        self.partial_response['data'] = response_data
    
    
    
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
    event = {"queryStringParameters": {"nit":"949439939-1", 'periodicity': "Anual" , "user": "dmanchola@precia.co"}}
    lambda_handler(event, '')
