
import datetime
import json
import logging
from queue import Queue
from threading import Thread
import sys
from boto3 import client as boto3_client
import sqlalchemy
import traceback
import pandas as pd
import os
from decorators import handler_wrapper, timing
from utils import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)


    
def lambda_handler(event, context):
    sc_obj = script_object(event)
    return sc_obj.starter()
    

class script_object:
    @handler_wrapper('inicializando objeto lambda','Objeto inicializado','Error tomando los valores de event','Problemas con los valores a calcular')
    def __init__(self, event):
        try:
            logger.info('[__INIT__] Inicializando objeto lambda ...')
            self.failed_init = False
            logger.info(f'event de entrada: {str(event)}')
            self.db_connection = 0
            self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'}}

            event_body_json = event["body"]
            event_body_dict = json.loads(event_body_json)
            self.nit = event_body_dict['nit']
            self.assessment_data = event_body_dict['assessment_data']
            self.id_assessment = self.assessment_data['ID']
            self.to_classify_short_date =  event_body_dict['date']
            self.to_classify_long_date =  datetime.datetime.strptime(event_body_dict['date'], '%d-%m-%Y').strftime('%Y-%m-%d %H:%M:%S')
            
            self.to_classify_periodicity = event_body_dict['periodicity']
            self.context = event_body_dict['context']

            self.classifications_to_avoid_depurate = ["Depreciación del periodo", "Depreciación acumulada", "Propiedad, planta y equipo", "Intangibles", "Amortización acumulada", "Amortización del periodo"]

            self.user_classified_df = pd.core.frame.DataFrame()
            self.default_classified_historic_df = pd.core.frame.DataFrame()
            self.archive_id = int()
            self.complete_classified_df = pd.core.frame.DataFrame()
            self.df_original = pd.core.frame.DataFrame()
            self.classification_summary_df = pd.core.frame.DataFrame()
            
            
        except Exception as e:
            self.failed_init = True
            logger.error(f"[__INIT__] error en inicializacion, linea: {get_current_error_line()}, motivo: "+str(e)) 
 

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError ('Error al inicializar lambda')
            
            self.create_conection_to_resources()
            self.acquire_id_archive()
            self.get_current_archive_clasification()
            self.get_initialy_classified_data()
            self.clasify_historic_data_with_current()
            self.summary_builder_caller()
            self.prepare_dataframe_to_front()
                
            self.db_connection.close()
            logger.info(f'[lambda_handler] Tareas de lambda terminadas satisfactoriamente')
            return self.response_maker(succesfull = True)
    
        except Exception as e:
            if self.db_connection:
                self.db_connection.close()  
            logger.error(f'[lambda_handler] Tareas de la lambda reportan errores fatales en el comando de la linea: {get_current_error_line()}, motivo: {str(e)}, creando respuesta...')
            return self.response_maker(succesfull = False, exception_str = str(e))



    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_resources(self):
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)
        
    @handler_wrapper('obteniendo Id archive de este hilo', 'Id archive obtenido con exito', 'Error obteniendo id_Archive', 'Error obteniendo id del archive del presente hilo')
    def acquire_id_archive(self):
        query = f"SELECT B.ID FROM COMPANY A, ARCHIVE B WHERE A.ID = B.ID_COMPANY AND A.NIT = '{self.nit}' AND B.INITIAL_DATE = '{self.to_classify_long_date}' LIMIT 1"
        logger.info(f"[acquire_id_archive] Query a base de datos para obtener el id_archive de este hilo {query}")
        self.current_thread_id_archive = self.db_connection.execute(query).scalar()
    

    @handler_wrapper('Buscando la clasificacion del analista','Clasificacion del analista encontrada','Error al buscar la clasificacion del analista','Error buscando la clasificacion del analista')
    def get_current_archive_clasification(self):
        query = f"SELECT A.CLASSIFICATION, B.ACCOUNT_NUMBER FROM RAW_CLASSIFICATION A, USER_CLASSIFICATION B WHERE A.ID = B.ID_RAW_CLASSIFICATION AND ID_ASSESSMENT = {self.assessment_data['ID']} ORDER BY ACCOUNT_NUMBER"
        logger.info(f"[get_current_archive_clasification] Query a base de datos para obtener la clasificacion del usuario {query}")
        self.user_classified_df = pd.read_sql(query, self.db_connection)


    @handler_wrapper('Obteniendo la informacion chequeada del puc','Informacion de puc encontrada', 'Error al hacer query de los datos chequeados','Error al buscar los datos de puc')
    def get_initialy_classified_data(self):
        data = {'pathParameters' : {'id_assessment' : self.id_assessment, 'id_archive': self.current_thread_id_archive}}
        logger.info(f'[]data que voy a lanzar a initial: {data}')
        #logger.warning(f"[get_initialy_classified_data] body que va a initial_classified:\n{data}\nEn la url:\n'{self.api_address}fincor/pucs/processed/classification'")
        data = json.dumps(data).encode()
        lambda_client = boto3_client('lambda')

        lambda_slave_initial_classification = os.environ['LAMBDA_SLAVE_INITIAL_CLASSIFICATION']
        invoke_response = lambda_client.invoke(FunctionName = lambda_slave_initial_classification,
                                           Payload=data)
    
        response_object = json.loads(json.loads(invoke_response['Payload'].read().decode())['body'])[0]

        del response_object['sector']
        dataframe_dict = response_object['data']

        self.default_classified_historic_df = pd.DataFrame.from_records(dataframe_dict)
        self.archive_id = response_object['archive_id']


    @handler_wrapper('Clasificando historico con actual','Data historica clasificada correctamente','Error al clasificar data historica','Error fatal al clasificar data historica')
    def clasify_historic_data_with_current(self):
        self.complete_classified_df = self.default_classified_historic_df.copy()
        for classified_row in self.user_classified_df.itertuples():
            self.complete_classified_df.loc[self.complete_classified_df['account'].str.startswith(classified_row[2]), 'classification'] = classified_row[1]

    @handler_wrapper('Enviando a calculo y guardado de summary e items','Guardado de resultados exitoso','Error calculando y guardando datos', 'Error guardando resultados')
    def summary_builder_caller(self):
        data = {'full_records':self.complete_classified_df.to_dict('records'), 'assessment_id':self.assessment_data['ID'], 'archive_id': self.current_thread_id_archive}
        logger.warning(f'[summary_builder_caller] body que va a builder:\n{data}')
        
        data = json.dumps({'body': json.dumps(data)}).encode()
        lambda_client = boto3_client('lambda')
        
        lambda_slave_summary_builder = os.environ['LAMBDA_SLAVE_SUMMARY_BUILDER']
        invoke_response = lambda_client.invoke(FunctionName = lambda_slave_summary_builder,
                                           Payload=data)
    
        response_object = json.loads(json.loads(invoke_response['Payload'].read().decode())['body'])


    @handler_wrapper('Preparando dataframe para front','Dataframe preparado con exito','Error al preparar dataframe','Error fatal al preparar dataframe')
    def prepare_dataframe_to_front(self):
        to_front_df = self.complete_classified_df.copy()
        to_front_df.fillna('No aplica', inplace=True)
        to_front_df.sort_values(by=['account'], inplace=True)
        to_front_df['nivel'] = to_front_df['account'].str.len()
        
        to_front_df['account2'] = to_front_df['account']
        to_front_df.set_index('account', inplace=True)
        to_front_df.rename(columns={'account2': 'account','account_name':'name'},inplace=True)
        logger.warning(f"Salida a front sin depurar y en records:\n{to_front_df.to_dict('records')}")
        self.final_response['body'] = {self.to_classify_short_date : to_front_df.to_dict('index')}
        

    def response_maker(self, succesfull = False, exception_str = str):

        if not succesfull:
            self.final_response['body'] = json.dumps(exception_str)
            return self.final_response
        else:
            self.final_response['body'] = json.dumps(self.final_response['body'])
            return self.final_response



def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)


