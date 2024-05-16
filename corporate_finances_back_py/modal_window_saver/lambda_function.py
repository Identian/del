#a esta lambda debo ponerle el sombrero de -to-db
import json
import logging
import sys
import os
import pandas as pd
from sqlalchemy import text
from datetime import datetime

from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db, connect_to_db_local_dev, call_dynamic_engine

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
            event_dict = json.loads(event['body'])
            self.id_assessment = event_dict['id_assessment']
            self.context = event_dict['context']
            self.projecting_items = event_dict['projection_data']
            
            self.historic_dates = list()
            self.projection_dates = list()
            self.years = int()
            self.accounts_original_values = dict()
            self.modal_window_records = []
            self.modal_projected_records = []


        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.create_conection_to_db()
            self.early_modal_windows_safe_delete()
            self.get_assessment_dates()
            self.create_db_records()
            self.upload_dataframes_to_bd()
            call_dynamic_engine(self.id_assessment, __name__)
            return self.response_maker(succesfull_run = True)
            
        except Exception as e:
            logger.error(f'[starter] Hubieron problemas en el comando de la linea: {get_current_error_line()}')
            return self.response_maker(succesfull_run = False, error_str = (str(e)))

    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_db(self):
        if __name__ != 'lambda_function':
            self.db_connection = connect_to_db_local_dev()
            self.db_connection.begin()
            return
        
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)


    @handler_wrapper('Obteniendo las fechas del proceso de valoración', 'Fechas del proceso de valroación obtenidas con exito', 'Error obteniendo las fechas del proceso de valoración', 'Error obteniendo fechas del proceso de valoración')
    def get_assessment_dates(self):
        query = f"SELECT DATES, PROPERTY FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY DATES"
        logger.info(f"[get_assessment_dates] Query a base de datos para obtener las fechas utilizadas en el proces de valoración:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        directory = {'HISTORIC': self.historic_dates, 'PROJECTION': self.projection_dates}
        for date_item in rds_data.fetchall():
            directory.get(date_item.PROPERTY, []).append(date_item.DATES.strftime('%Y-%m-%d %H:%M:%S'))

        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')
        

    @handler_wrapper('Fue necesaria la eliminacion de proyecciones en BD', 'Eliminación exitosa', 'Error eliminado proyecciones de BD', 'Error sobreescribiendo proyecciones en bd')
    def early_modal_windows_safe_delete(self):
        query = f'DELETE FROM MODAL_WINDOWS WHERE ID_ASSESSMENT = {self.id_assessment} AND CONTEXT_WINDOW = "{self.context}"'
        logger.warning(f'[early_modal_windows_safe_delete] Query para eliminacion de datos anteriores para el contexto {self.context}: \n{query}')
        self.db_connection.execute(text(query))

        query = f'DELETE FROM MODAL_WINDOWS_PROJECTED WHERE ID_ASSESSMENT = {self.id_assessment} AND CONTEXT_WINDOW = "{self.context}"'
        logger.warning(f'[early_modal_windows_safe_delete] Query para eliminacion de datos anteriores para el contexto {self.context}: \n{query}')
        self.db_connection.execute(text(query))
        
            
    @handler_wrapper('Organizando records para envio a bd', 'Records alistados con exito', 'Error areglando records para envío a bd', 'Error manejando datos para envío a bd')
    def create_db_records(self):
        logger.info(f'[create_db_records] array de objetos a organizar para bd:\n{self.projecting_items}')
        for item in self.projecting_items:
            self.modal_window_records.append({'ID_ASSESSMENT': self.id_assessment, 
                                                'ORIGINAL_VALUE': 0, 
                                                'ACCOUNT_NUMBER': item['account'], 
                                                'CONTEXT_WINDOW': self.context, 
                                                'VS_ACCOUNT_NAME': item['accountProjector'], 
                                                'PROJECTION_TYPE': item['method'], 
                                                'COMMENT':item['explication']})
            
            projection_records = [{'ID_ASSESSMENT': self.id_assessment, 
            'VALUE': 0, 
            'ACCOUNT_NUMBER': item['account'], 
            'CONTEXT_WINDOW': self.context, 
            'PROJECTED_DATE': year,
            'ATRIBUTE': item['atributes']['projection'][i]
            } for i, year in enumerate(self.projection_dates)]
            self.modal_projected_records.extend(projection_records)


    @handler_wrapper('Cargando data a bd', 'Data carga a bd', 'Error en la carga de información a bd', 'Error cargando la información a bd')
    def upload_dataframes_to_bd(self):
        self.modal_window_df = pd.DataFrame.from_records(self.modal_window_records)
        self.modal_projected_df = pd.DataFrame.from_records(self.modal_projected_records)
        logger.info(f'[upload_dataframes_to_bd] Dataframe que se cargará a MODAL_WINDOWS:\n{self.modal_window_df.to_string()}')
        logger.info(f'[upload_dataframes_to_bd] Dataframe que se cargará a MODAL_WINDOWS_PROJECTED:\n{self.modal_projected_df.to_string()}')
        self.modal_window_df.to_sql(name='MODAL_WINDOWS', con=self.db_connection, if_exists='append', index=False)
        self.modal_projected_df.to_sql(name='MODAL_WINDOWS_PROJECTED', con=self.db_connection, if_exists='append', index=False)


    def response_maker(self, succesfull_run=False, error_str=""):
        if self.db_connection:
            self.db_connection.close()
        if not succesfull_run:
            self.final_response["body"] = json.dumps(self.detailed_raise if self.detailed_raise else error_str)
        else:
            self.final_response["statusCode"] = 200
            self.final_response["body"] = json.dumps("Ok")
        return self.final_response



def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    

if __name__ == '__main__':
    event = {'body': '{"context": "patrimony", "datesHistory": ["2023-12-31", "2024-12-31"], "dates_projections": ["2025", "2026", "2027"], "id_assessment": "2064", "projection_data": [{"account": "23", "name": "patrimonio (23)", "accountProjector": "No aplica", "atributes": {"history": [64099.94, 32049.97], "projection": [10.0, 20.0, 30.0]}, "explication": "Hola", "method": "Tasa de crecimiento fija"}, {"account": "11", "name": "caja (11)", "accountProjector": "No aplica", "atributes": {"history": [64099.94, 32049.97], "projection": ["", "", ""]}, "explication": "Proyecci\\u00f3n primera vez, debug", "method": "Cero"}], "year": 3}'}
    lambda_handler(event, '')