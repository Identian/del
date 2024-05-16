import datetime
import json
import logging
import os
import sys
import traceback

import pandas as pd
from sqlalchemy import text
from decorators import debugger_wrapper, handler_wrapper
from utils import connect_to_db, get_secret, local_connect_to_db

logging.basicConfig()
logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(module)s] %(message)s', force=True, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


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
            self.partial_response = {}

            logger.warning(f'event de entrada: {str(event)}')
            self.id_assessment = json.loads(event['body'])['id_assessment']
            
            
            self.simple_atributes_tables = {'CAPEX': ['USED_ACCOUNT_NAME', 'METHOD', 'TIME_PERIOD', 'PERIODS', 'CALCULATION_COMMENT'],
                                'DEBT': ['ORIGINAL_VALUE', 'ACCOUNT_NUMBER', 'ALIAS_NAME', 'PROJECTION_TYPE', 'START_YEAR', 'ENDING_YEAR', 'DEBT_COMMENT', 'RATE_COMMENT', 'SPREAD_COMMENT'],
                                'FIXED_ASSETS': ['ID_ITEMS_GROUP', 'PROJECTION_TYPE', 'ASSET_ACCOUNT', 'ACUMULATED_ACCOUNT', 'PERIOD_ACCOUNT', 'PROJECTED_YEARS', 'CALCULATION_COMMENT'],
                                'MODAL_WINDOWS': ['ACCOUNT_NUMBER', 'CONTEXT_WINDOW', 'VS_ACCOUNT_NAME', 'PROJECTION_TYPE', 'COMMENT'],
                                'PYG_ITEM': ['ID_RAW_PYG', 'ID_DEPENDENCE', 'PROJECTION_TYPE', 'COMMENT'],
                                'USER_CLASSIFICATION': ['ACCOUNT_NUMBER', 'ID_RAW_CLASSIFICATION'],
                                'CALCULATED_ASSESSMENT': ['ASSESSMENT_DATE', 'INITIAL_DATE', 'DATES_ADJUST_ATRIBUTE', 'DATES_ADJUST_COMMENT', 'CHOSEN_FLOW_NAME', 'GRADIENT', 'TERMINAL_VALUE', 'TOTAL_NOT_OPERATIONAL_ASSETS', 'TOTAL_OPERATIONAL_PASIVES', 'OUTSTANDING_SHARES', 'ADJUST_METHOD']}
                                
                                
            self.ordered_atributes_tables = {'CAPEX_VALUES': {'columns': ['CALCULATED_DATE', 'MANUAL_PERCENTAGE', 'CAPEX_SUMMARY', 'CAPEX_ACUMULATED'], 'order':'CALCULATED_DATE'},
                                'PROJECTED_DEBT': {'columns': ['ITEM_DATE', 'ACCOUNT_NUMBER', 'ALIAS_NAME', 'INITIAL_BALANCE', 'DISBURSEMENT', 'AMORTIZATION', 'ENDING_BALANCE', 'INTEREST_VALUE','ENDING_BALANCE_VARIATION', 'RATE_ATRIBUTE', 'SPREAD_ATRIBUTE'], 'order':'ITEM_DATE'},
                                'FCLO_DISCOUNT': {'columns': ['ITEM_DATE', 'DISCOUNT_PERIOD', 'DISCOUNT_RATE', 'DISCOUNT_FACTOR'], 'order':'ITEM_DATE'},
                                'PROJECTED_FIXED_ASSETS': {'columns': ['PROJECTED_DATE', 'ID_ITEMS_GROUP', 'ASSET_VALUE', 'ACUMULATED_VALUE', 'EXISTING_ASSET_VALUE', 'PERIOD_VALUE'], 'order':'PROJECTED_DATE'},
                                'MODAL_WINDOWS_PROJECTED': {'columns': ['PROJECTED_DATE', 'ACCOUNT_NUMBER', 'CONTEXT_WINDOW', 'ATRIBUTE', 'VALUE'], 'order': 'PROJECTED_DATE'},
                                'PROJECTED_PYG': {'columns': ['PROJECTED_DATE', 'ID_RAW_PYG', 'ATRIBUTE', 'VALUE'], 'order':'PROJECTED_DATE'}}
            
            self.columns_dropper = {}
            
            self.historic_dates = list()
            self.projection_dates = list()
            self.total_asssessment_dates = 0
            self.table_atributes = dict()
            self.table_dfs = dict()
            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True


    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
                
            logger.info('[starter] Empezando starter de objeto lambda')
            self.create_conection_to_db()
            self.get_company_id()
            self.get_assessment_dates()
            self.acquire_assessment_simple_attributes()
            self.acquire_assessment_ordered_attributes()
            self.reduce_model_dates()
            self.create_uploable_dataframes()
            self.delete_previous_models()
            self.upload_models()
            #self.db_connection.commit()
            return self.response_maker(succesfull_run = True)
        except Exception as e:
            logger.error(f'[starter] Hubieron problemas en el comando de la linea: {get_current_error_line()}')
            return self.response_maker(succesfull_run = False, error_str = (str(e)))


    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_db(self):
        if __name__ == "__main__":
            self.db_connection = local_connect_to_db()
        else:
            db_schema = os.environ['DB_SCHEMA']
            secret_db_region = os.environ['SECRET_DB_REGION']
            secret_db_name = os.environ['SECRET_DB_NAME']
            db_secret = get_secret(secret_db_region, secret_db_name)
            self.db_connection = connect_to_db(db_schema, db_secret)


    @handler_wrapper('Obteniendo el ID de la empresa', 'ID de la empresa obtenido con exito', 'Error obteniendo ID de la empresa', 'Error obteniendo identificador de empresa')
    def get_company_id(self):
        query = 'SELECT A.ID FROM COMPANY A, ARCHIVE B, ASSESSMENT C WHERE A.ID = B.ID_COMPANY AND B.ID = C.ID_ARCHIVE AND C.ID = :id_assessment'
        logger.info(f'[get_company_id] Query para obtener el ID de la empresa que se está valorando: {query}')
        rds_data = self.db_connection.execute(text(query), {'id_assessment': self.id_assessment})
        self.id_company = int(rds_data.scalar())
        

    @handler_wrapper('Obteniendo las fechas del proceso de valoración', 'Fechas del proceso de valroación obtenidas con exito', 'Error obteniendo las fechas del proceso de valoración', 'Error obteniendo fechas del proceso de valoración')
    def get_assessment_dates(self):
        query = "SELECT DATES, PROPERTY FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = :id_assessment ORDER BY DATES"
        logger.info(f"[get_assessment_dates] Query a base de datos para obtener las fechas utilizadas en el proces de valoración:\n {query}")
        rds_data = self.db_connection.execute(text(query), {'id_assessment': self.id_assessment})
        directory = {'HISTORIC': self.historic_dates, 'PROJECTION': self.projection_dates}
        for date_item in rds_data.fetchall():
            directory.get(date_item.PROPERTY, []).append(date_item.DATES)
            self.total_asssessment_dates = self.total_asssessment_dates +1
            
        self.historic_dates = [date.strftime('%Y-%m-%d %H:%M:%S') for date in self.historic_dates]
        self.projection_dates = [date.strftime('%Y-%m-%d %H:%M:%S') for date in self.projection_dates]
        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')
    
    
    @handler_wrapper('Obteniendo Atributos simples del proceso de valoración', 'Atributos simples del proceso de valoración obtenidos con exito', 'Error obteniendo los atributos simples del proceso de valoración', 'Error obteniendo atributos del proceso de valoración')
    def acquire_assessment_simple_attributes(self):
        for table, columns in self.simple_atributes_tables.items():
            cleaned_colums = str(columns).replace('[','').replace(']','').replace("'", '')
            query = f"SELECT {cleaned_colums} FROM {table} WHERE ID_ASSESSMENT = {self.id_assessment}"
            logger.info(f"[acquire_assessment_simple_attributes] Query a base de datos para obtener los atributos de la tabla {table}:\n{query}")
            rds_data = self.db_connection.execute(text(query))
            self.table_atributes[table] = [row._asdict() for row in rds_data.fetchall()]
            logger.info(f'[acquire_assessment_simple_attributes] Datos encontrados para la tabla {table}:\n{self.table_atributes[table]}')


    @handler_wrapper('Obteniendo Atributos del proceso de valoración', 'Atributos del proceso de valoración obtenidos con exito', 'Error obteniendo atributos del proceso de valoración', 'Error obteniendo atributos del proceso de valoración')
    def acquire_assessment_ordered_attributes(self):
        for table, cols_order in self.ordered_atributes_tables.items():
            cleaned_colums = str(cols_order['columns']).replace('[','').replace(']','').replace("'", '')
            query = f"SELECT {cleaned_colums} FROM {table} WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY {cols_order['order']}"
            logger.info(f"[acquire_assessment_ordered_attributes] Query a base de datos para obtener los atributos de la tabla {table}:\n{query}")
            rds_data = self.db_connection.execute(text(query))
            self.table_atributes[table] = [row._asdict() for row in rds_data.fetchall()]
            logger.info(f'[acquire_assessment_ordered_attributes] Datos encontrados para la tabla {table}:\n{self.table_atributes[table]}')
    
    
    @handler_wrapper('Reduciendo cantidad de fechas historicas y proyectadas', 'Reduccion de fechas exitosa', 'Error reduciendo cantidad de fechas', 'Error Revisando fechas del proceso de valoración')
    def reduce_model_dates(self):
        assessment_has_annual_attributes = 1 if self.historic_dates[-1].split('-')[1] != '12' else 0
        self.table_atributes['ASSESSMENT_YEARS'] = [{'HISTORIC_YEARS':len(self.historic_dates), 'PROJECTION_YEARS': len(self.projection_dates), 'ANNUAL_ATTRIBUTE': assessment_has_annual_attributes}]
        #self.table_atributes['ASSESSMENT_YEARS'] = [{'HISTORIC_YEARS':len(self.historic_dates), 'PROJECTION_YEARS': len(self.projection_dates)}]
        
    @handler_wrapper('Creando dataframes de carga a bd', 'Dataframes de carga creados con exito', 'Error creando dataframes de carga a bd', 'Error creando tablas modelo')
    def create_uploable_dataframes(self):
        for table, atribute_list in self.table_atributes.items():
            self.table_dfs[table] = pd.DataFrame.from_records(atribute_list)
            
    
    @handler_wrapper('Eliminando posibles modelos anteriores', 'Eliminación de modelos anteriores exitosa', 'Error eliminando posibles modelos anteriores', 'Error sobreescribiendo data')    
    def delete_previous_models(self):
        for table in self.table_dfs:
            query = f"DELETE FROM MODEL_{table} WHERE ID_COMPANY = {self.id_company}"
            logger.info(f"[delete_previous_models] Query a base de datos para eliminar el modelo de la tabla {table}:\n{query}")
            self.db_connection.execute(text(query))

         
    @handler_wrapper('Cargando modelos a bd', 'Modelos cargados a bd con exito', 'Error cargando modelos a bd', 'Error cargando modelos a base de datos')
    def upload_models(self):
        for table, df in self.table_dfs.items():
            df['ID_COMPANY'] = self.id_company
            logger.info(f'[upload_models] Cargando el siguiente dataframe en la tabla MODEL_{table}:\n{df.to_string()}')
            df.to_sql(name= f'MODEL_{table}', con=self.db_connection, if_exists='append', index=False)
    
    
    @debugger_wrapper('Error construyendo respuesta final', 'Error construyendo respuesta')
    def response_maker(self, succesfull_run = False, error_str = str()):
        if self.db_connection:
            self.db_connection.close()
        if succesfull_run:
            self.final_response['statusCode'] = 200
            self.final_response['body'] = json.dumps('ok')
            return self.final_response
            
        self.final_response['body'] = json.dumps(self.detailed_raise if self.detailed_raise else error_str)
        return self.final_response


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)

if __name__ == "__main__":
    event = {'body': '{"id_assessment": 30}'}
    lambda_handler(event, '')