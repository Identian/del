""":
capas:
capa-pandas-data-transfer

variables de entorno:
ARCHIVES_TABLE : ARCHIVE
ASSESSMENT_TABLE : ASSESSMENT
CALCULATED_ASSESSMENT_TABLE : CALCULATED_ASSESSMENT
CASH_FLOW_TABLE : CASH_FLOW
COMPANIES_TABLE : COMPANY
DB_SCHEMA : src_corporate_finance
FCLO_DISCOUNT_TABLE : FCLO_DISCOUNT
RAW_CASH_FLOW_TABLE : RAW_CASH_FLOW
SECRET_DB_NAME : precia/rds8/sources/finanzas_corporativas
SECRET_DB_REGION : us-east-1

RAM: 1024 MB

"""
import json
import logging
import sys
import traceback
import time
import datetime
import copy
import os 
from utils import *
import pandas as pd
from decorators import handler_wrapper, timing

#logging.basicConfig() #En lambdas borra este
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
        self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 
                                               'Access-Control-Allow-Origin': '*', 
                                              'Access-Control-Allow-Methods': '*'}, "statusCode": 500, 'body': {}}
        self.failed_init = False
        try:
            logger.warning(f'[__init__] event que llega a la lambda: {str(event)}')
    
            event_body_json = event["body"]
            event_dict = json.loads(event_body_json)
            
            self.id_assessment = event_dict['id_assessment']
            self.close_assessment = event_dict['isFinal']
            self.fclo_data = event_dict['valuation']['flowDiscount'][0]
            self.assessment_data = event_dict['valuation']
            
            self.assessment_insert_str = f"INSERT INTO CALCULATED_ASSESSMENT VALUES ("
            self.search_directory = {'ASSESSMENT_DATE-date' : 'dateAssessment', 
            'INITIAL_DATE-date' : 'dateInitial',
            'CURRENT_CLOSING_DATE-date' : 'dateCurrentClosing',
            'FLOW_HALF_PERIOD-date' : 'flowHalfPeriod',
            'NEXT_FLOW_HALF_YEAR-date' : 'nextFlowHalfYear',
            'DATES_ADJUST_ATRIBUTE-varchar' : 'adjust',
            'DATES_ADJUST_COMMENT-varchar' : 'explicationDateAdjust',
            'CHOSEN_FLOW_NAME-varchar' : 'cashFree',
            'CHOSEN_FLOW_COMMENT-varchar' : 'force_False', #no existe en front
            'DISCOUNT_RATE_COMMENT-varchar' : 'force_False', #no existe en front
            'VP_FLOWS-decimal' : 'vpFlows',
            'GRADIENT-varchar' : 'gradient',
            'NORMALIZED_CASH_FLOW-decimal' : 'normalicedCashFlow',
            'DISCOUNT_RATE_ATRIBUTE-varchar' : 'discountRateAtribute',
            'TERMINAL_VALUE-decimal' : 'terminalValue',
            'DISCOUNT_FACTOR-varchar' : 'discountFactor',
            'VP_TERMINAL_VALUE-decimal' : 'vpTerminalValue',
            'ENTERPRISE_VALUE-decimal' : 'enterpriseValue',
            'FINANCIAL_ADJUST-decimal' : 'financialAdjust', #este falta que lo mande el front
            'TOTAL_NOT_OPERATIONAL_ASSETS-decimal' : 'activesNoOperational',
            'TOTAL_OPERATIONAL_PASIVES-decimal' : 'pasiveOperational',
            'ASSETS_COMMENT-varchar' : 'assestComment', #falta
            'PASIVES_COMMENT-varchar' : 'pasiveComment', #falta
            'PATRIMONY_VALUE-decimal' : 'patrimonial',
            'OUTSTANDING_SHARES-int' : 'nActions',
            'ASSESSMENT_VALUE-decimal' : 'valuePerValuation',
            'ADJUST_METHOD-varchar':'method'
            }
            
            self.db_connection = 0
            self.historic_dates = list()
            self.projection_dates = list()
            
            logger.warning(f'[__init__] Inicialización terminada')

        except Exception as e:
            logger.error(f'[__init__] Error en la inicialicación, motivo: {str(e)}, linea: {get_current_error_line()}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error de inicializacion')
                
            logger.info(f'[starter] Empezando starter de objeto lambda')
            self.create_conection_to_resources()
            self.get_assessment_dates()
            self.check_existant_calculated_assessment_data()
            self.assessment_data_organizer()
            self.organize_fclo()
            self.upload_data()
            self.save_assessment_step()
            if self.close_assessment:
                self.close_assessment_process()
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


    @handler_wrapper('Obteniendo las fechas del proceso de valoración', 'Fechas del proceso de valroación obtenidas con exito', 'Error obteniendo las fechas del proceso de valoración', 'Error obteniendo fechas del proceso de valoración')
    def get_assessment_dates(self):
        query = f"SELECT DATES, PROPERTY FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY DATES"
        logger.info(f"[get_assessment_dates] Query a base de datos para obtener las fechas utilizadas en el proces de valoración:\n {query}")
        rds_data = self.db_connection.execute(query)
        directory = {'HISTORIC': self.historic_dates, 'PROJECTION': self.projection_dates}
        found_dates = [row._asdict() for row in rds_data.fetchall()]
        for date_item in found_dates:
            directory.get(date_item['PROPERTY'], []).append(date_item['DATES'].strftime('%Y-%m-%d %H:%M:%S'))
        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')
        

    @handler_wrapper('Chequeando si hay datos para este proceso de valoracion','Chequeo de datos anteriores terminado','Error revisando existencia de datos anteriores en bd','Error revisando si hay datos anteriores')
    def check_existant_calculated_assessment_data(self):
        logger.info(f'Prueba para saber existencia de calculated_assessment:')
        query = f"SELECT EXISTS (SELECT * FROM CALCULATED_ASSESSMENT WHERE ID_ASSESSMENT = {self.id_assessment} LIMIT 1)"
        logger.info(f'este es el query: \n{query}')
        respuesta_exists = self.db_connection.execute(query).scalar() #esto está en pruebas de concepto, revisar si se puede mandar directo al if
        logger.warning(f'esta fue la respuesta: \n{respuesta_exists} de tipo {type(respuesta_exists)}')
        
        if respuesta_exists:
            logger.error('El proceso de valoracion ya está publicado, no se puede reemplazar')
            #raise AttributeError('El proceso de valoracion ya está publicado, no se debe reemplazar')
            self.calculated_assessment_safe_delete()
            
    
    #este deja de usarse porque el proceso de valoración va a qudar en status Issued automaticamente
    @handler_wrapper('El proceso requirió la eliminacion de proyecciones anteriores','Borrado seguro realizado con exito','Error realizando el borrado seguro de proyecciones anteriores','Problemas eliminando proyecciones anteriores')
    def calculated_assessment_safe_delete(self):
        query = f"DELETE FROM CALCULATED_ASSESSMENT WHERE ID_ASSESSMENT = {self.id_assessment}"
        logger.info(f'[calculated_assessment_safe_delete] Query para eliminar datos anteriores de tabla CALCULATED_ASSESSMENT: {query}')
        self.db_connection.execute(query)
        
        query = f"DELETE FROM FCLO_DISCOUNT WHERE ID_ASSESSMENT = {self.id_assessment}"
        logger.info(f'[calculated_assessment_safe_delete] Query para eliminar datos anteriores de tabla FCLO_DISCOUNT: {query}')
        self.db_connection.execute(query)

    
    @handler_wrapper('Acomodando informacion de valoración para enviar a db','String preparado para envir a bd','Error organizando información','Error procesando información de valoracion')
    def assessment_data_organizer(self):
        self.assessment_insert_str += str(self.id_assessment)
        temp_dict = {}
        logger.info(f'[mira aca]{self.search_directory}')
        for key, value in self.search_directory.items():
            logger.warning(f'[assessment_data_organizer] buscando {value} en:\n{self.assessment_data}')
            temp_value = self.assessment_data.get(value, False)
            temp_dict[key] = temp_value
            logger.info(f'{key} valor: {temp_value}')
            if not temp_value and temp_value != 0:
                self.assessment_insert_str += f', NULL '
                continue
            if key.endswith('-date'):
                temp_date_long_str = datetime.datetime.strptime(temp_value, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')
                self.assessment_insert_str += f', "{temp_date_long_str}"'

            elif key.endswith('-varchar'):
                self.assessment_insert_str += f', "{temp_value}"'
            
            elif key.endswith('-decimal') or key.endswith('-int'):
                self.assessment_insert_str += f', {temp_value}'
        self.assessment_insert_str += ')'
        logger.warning(temp_dict)


    @handler_wrapper('Organizando tabla de FCLO', 'Tabla de FCLO organizada con exito', 'Error organizando tabla de FCLO', 'Error procesando datos de valoracion')
    def organize_fclo(self):
        temp_date = self.historic_dates[-1:] + self.projection_dates if '-12-' in self.historic_dates[-1] else self.projection_dates
        
        self.fclo_data['ITEM_DATE'] = temp_date
        self.fclo_df = pd.DataFrame.from_dict(self.fclo_data)
        self.fclo_df['ID_ASSESSMENT'] = self.id_assessment
        self.fclo_df.astype({'operativeCashFlow': 'float', 
                    'discountPeriod': 'string', 
                    'discountRate': 'string', 
                    'discountFactor': 'string', 
                    'fclo': 'float', })
        self.fclo_df.drop(['dates'], axis=1, inplace = True)

        self.fclo_df.rename(columns = {'operativeCashFlow':'OPERATIVE_CASH_FLOW', 
                                            'discountPeriod':'DISCOUNT_PERIOD',
                                            'discountRate':'DISCOUNT_RATE',
                                            'discountFactor':'DISCOUNT_FACTOR',
                                                    'fclo':'FCLO'}, inplace = True)
                                                    
    
    @handler_wrapper('Subiendo datos a base de datos','Datos subidos a bd con exito','Error subiendo datos a bd','Error subiendo datos a bd')
    def upload_data(self):
        logger.warning(f'[upload_data] query de linea de valoracion:\n{self.assessment_insert_str}')
        logger.warning(f'[upload_data] Dataframe a cargar a fclo: \n{self.fclo_df.to_string()}')
        
        self.db_connection.execute(self.assessment_insert_str)
        logger.warning(f'[upload_data] Carga de linea de valoracion exitosa')
        
        #query = f"UPDATE ASSESSMENT SET `STATUS` = \"Issued\" WHERE ID = {self.id_assessment}"
        #logger.info(f'[upload_data] Query para cabiar el status del proceso de valoración: {query}')
        #self.db_connection.execute(query)

        self.fclo_df.to_sql(name='FCLO_DISCOUNT', con=self.db_connection, if_exists='append', index=False)   
        

    @handler_wrapper('Guardando el paso del proceso de valoracion','Paso guardado correctamente','Error guardando el paso del proceso de valoración', 'Error guardando informacion')
    def save_assessment_step(self):
        try:
            query = f"INSERT INTO ASSESSMENT_STEPS VALUES ({self.id_assessment}, \"VALORATION\");"
            logger.info(f"[save_assessment_step] Query a base de datos para guardar el paso del proceso de valoracion: \n{query}")
            rds_data = self.db_connection.execute(query)
        except Exception as e:
            logger.warning(f'[save_assessment_step] Es posible que el step del proceso de valoracion ya haya sido guardado, sin embargo, este es el mensaje de error:\n{str(e)}')


    @handler_wrapper('Cerrando proceso de valoración', 'Proceso de valroación cerrado correctamente', 'Error cambiando status del proceso de valoración', 'Error cambiando status del proceso de valoración, proceso no fue cerrado correctamente')
    def close_assessment_process(self):
        query = f"UPDATE ASSESSMENT SET STATUS = 'Published' WHERE ID = {self.id_assessment}"
        logger.info(f'[close_assessment_process] Query para cerrar el proceso de valoración:\n{query}')
        self.db_connection.execute(query)
        

    def response_maker(self, succesfull = False, exception_str = str):
        if not succesfull:
            self.final_response['body'] = json.dumps(exception_str)
            return self.final_response
        else:
            self.final_response['statusCode'] = 200
            self.final_response['body'] = json.dumps(f"Guardado en bd satisfactorio")
            return self.final_response



def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)

 

        