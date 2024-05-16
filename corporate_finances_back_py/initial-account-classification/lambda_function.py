#{"body": "{\"nit\":\"800149923-8\",\"date\":\"30-11-2022\",\"periodicity\":\"Trimestral\"}"}


import boto3
import datetime
import json
import logging
import sys
import traceback
import os 

import pandas as pd

from utils import *
from decorators import handler_wrapper, timing


logger = logging.getLogger()
logger.setLevel(logging.INFO)

 
def lambda_handler(event, context):
    sc_obj = script_object(event)
    lambda_response = sc_obj.starter()
    logger.info(f'respuesta final de lambda: \n{lambda_response}')
    return lambda_response
    
    
class script_object:
    def __init__(self, event):
        try:
            self.failed_init = False

            logger.info('[__INIT__] Inicializando objeto lambda ...')
            logger.info(f'event de entrada_ {str(event)}')


            logger.warning(f'event de entrada: {str(event)}')
 
            self.puc_chapters = {'1':'Activo', '2':'Pasivo', '3':'Patrimonio', '4':'Ingresos', '5':'Gastos', '6':'Costos de venta', '7':'Costos de producción o de operación', '8':'Cuentas de orden deudoras', '9':'Cuentas de orden acreedoras'}
            self.status_dict = {'No clasificado': False}


            event_dict = event['pathParameters']

            self.id_assessment = event_dict['id_assessment']
            
            self.id_archive = event_dict.get('id_archive', False)

            self.db_connection = 0
            self.s3_client = 0
            
            self.puc_chapters = {'1':'Activo', '2':'Pasivo', '3':'Patrimonio', '4':'Ingresos', '5':'Gastos', '6':'Costos de venta', '7':'Costos de producción o de operación', '8':'Cuentas de orden deudoras', '9':'Cuentas de orden acreedoras'}
        
            self.company_info = dict()
            self.archive_data = dict()
            self.company_has_model = False
            
            self.puc_data_on_db = pd.core.frame.DataFrame()
            self.puc_data = pd.core.frame.DataFrame()
            self.classified_df = pd.core.frame.DataFrame()
            
            self.df_classified_dict = dict() #este es el que viaja al front
            
            self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'}, 'statusCode': 200}
            logger.info('[__INIT__] Objeto lambda inicializada exitosamente')
            
        except Exception as e:

            self.failed_init = True
            logger.error(f"[__INIT__] error en inicializacion, linea: {get_especific_error_line()}, motivo: "+str(e)) 


    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Falla inicializacion, revisar logs')
            self.create_clients()
            self.get_company_info()
            #self.get_archive_info()
            self.get_puc_data()
            self.setting_up_df()
            self.check_company_model()
            if not self.company_has_model:
                self.get_classification_dict()
            
            self.classify_dataframe()
            self.clean_df_to_front()
            return self.response_maker(succesfull = True)
            
        except Exception as e:
            logger.error(f'[starter] Error en el procesamieno del comando de la linea: {get_current_error_line()}, motivo: {e}')
            return self.response_maker(succesfull = False, exception_str = str(e))


    @handler_wrapper('Creando clientes a servicios externos','Clientes a servicios construidos','Error construyendo conexiones a recursos externos','Problemas requiriendo recursos externos')
    def create_clients(self):
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)
        self.s3_client = boto3.client('s3')


    @handler_wrapper('Buscando id del nit recibido','El id del nit recibido fue encontrado','Error en la busqueda de los datos de la empresa','Error, problemas localizando la informacion de la empresa')
    def get_company_info(self):
        query = f"SELECT A.ID, A.NIT, A.SECTOR, C.ID_ARCHIVE, B.INITIAL_DATE FROM COMPANY A, ARCHIVE B, ASSESSMENT C WHERE A.ID = B.ID_COMPANY AND B.ID = C.ID_ARCHIVE AND C.ID = {self.id_assessment} LIMIT 1"
        logger.info(f"Query a base de datos para obtener la informacion de la empresa: {query}")
        rds_data = self.db_connection.execute(query)
        self.assessment_info = [row._asdict() for row in rds_data.fetchall()][0]
        self.initial_date_short = self.assessment_info['INITIAL_DATE'].strftime('%d-%m-%Y')


    @handler_wrapper('Obteniendo la informacion chequeada del puc','Informacion de puc encontrada', 'Error al hacer query de los datos chequeados','Error al buscar los datos de puc')
    def get_puc_data(self):
        if self.id_archive:
            query = f"SELECT ACCOUNT_NUMBER, CHECKED_BALANCE, ACCOUNT_NAME FROM ORIGINAL_PUC WHERE ID_ARCHIVE = {self.id_archive} ORDER BY ACCOUNT_NUMBER"
        else:
            query = f"SELECT ACCOUNT_NUMBER, CHECKED_BALANCE, ACCOUNT_NAME FROM ORIGINAL_PUC WHERE ID_ARCHIVE = {self.assessment_info['ID_ARCHIVE']} ORDER BY ACCOUNT_NUMBER"
        logger.info(f"Query a base de datos para obtener la data del puc {query}")
        self.puc_data_on_db = pd.read_sql(query, self.db_connection)


    @handler_wrapper('Cambiando tipos de datos en las columnas','Configuracion de dataframe completado','Error en la preparación del dataframe','Error en la preparación del dataframe')
    def setting_up_df(self):
        self.puc_data = self.puc_data_on_db.copy()
        self.puc_data.rename(columns={'ACCOUNT_NUMBER': 'account', 'ACCOUNT_NAME': 'name', 'CHECKED_BALANCE':'balance'}, inplace=True)
        self.puc_data['nivel'] = self.puc_data['account'].str.len()
        self.puc_data['initial_date'] = self.initial_date_short
        self.puc_data = self.puc_data.astype({"account": "string", "name": "string", "balance": "float", "nivel":"int"}, copy=True)
        

    @handler_wrapper('Chequeando si la empresa tiene un modelo de clasificación construído', 'Chequeo de modelo de clasificación previa terminado','Error chequeando si la empresa tiene un model de clasificación construído', 'Error chequeando si existe modelo de clasificación previo')
    def check_company_model(self):
        query = f"SELECT A.ACCOUNT_NUMBER, B.CLASSIFICATION FROM MODEL_USER_CLASSIFICATION A, RAW_CLASSIFICATION B WHERE A.ID_RAW_CLASSIFICATION = B.ID AND A.ID_COMPANY = {self.assessment_info['ID']} ORDER BY A.ACCOUNT_NUMBER"
        logger.info(f"[check_company_model] Query a base de datos para obtener la data del puc {query}")  
        rds_data = self.db_connection.execute(query)
        self.classification_list = rds_data.mappings().all()
        logger.info(f'[check_company_model] Este fue el model encontrado para la empresa que se está valorando:\n{self.classification_list}')
        if self.classification_list:
            self.company_has_model = True
        
        
    @handler_wrapper('Leyendo archivo de clasificacion','Archivo de clasificacion leído satisfactoriamente','No se pudo leer el archivo de clasificacion','Error en la generacion de archivo de clasificacion')
    def get_classification_dict(self):
        query = f"SELECT A.CLASSIFICATION, B.ACCOUNT_NUMBER FROM RAW_CLASSIFICATION A, DEFAULT_CLASSIFICATION B WHERE A.ID = B.ID_RAW_CLASSIFICATION AND SECTOR = \"{self.assessment_info['SECTOR']}\" ORDER BY 2"
        logger.info(f"Query a base de datos para obtener la data del puc {query}")  
        rds_data = self.db_connection.execute(query)
        self.classification_list = rds_data.mappings().all()
        #logger.warning(f'Data obtenida del pedido de clasificaciones: {self.classification_list}')
        
        
    @handler_wrapper('Empieza clasificacion','Dataframe clasificado satisfactoriamente','Error en la clasificacion del dataframe','No se pudo realizar la clasificacion')
    def classify_dataframe(self):
        self.classified_df = self.puc_data.copy()
        for default_row in self.classification_list:
            self.classified_df.loc[self.classified_df['account'].str.startswith(default_row['ACCOUNT_NUMBER']), 'classification'] = default_row['CLASSIFICATION']

        
    @handler_wrapper('Organizando dataframe para muestra en front','Dataframe organizado correctamente','Error organizando datos para muestra en front','Hubieron problemas preparando la informacion clasificada')
    def clean_df_to_front(self):
        #self.classified_df.fillna('No aplica', inplace=True)
        self.classified_df.loc[self.classified_df['nivel'] == 1, 'classification' ] = 'No aplica'
        self.classified_df.fillna(value={'classification': 'No clasificado'}, inplace = True)
        #self.classified_df['status'] = 
        self.classified_df.loc[self.classified_df['classification'] == 'No clasificado', 'status' ] = False
        self.classified_df.loc[self.classified_df['classification'] != 'No clasificado', 'status' ] = True

        self.classified_df.sort_values(by=['account'], inplace=True)
        
        self.classified_df['chapter'] = [self.puc_chapters[account_number[0]] for account_number in self.classified_df['account'].values.tolist() ]
        
        #self.classified_df.set_index('account', inplace=True)
        self.df_classified_dict = self.classified_df.to_dict('records')
        
        
    def response_maker(self, succesfull = False, exception_str = str()):
        if self.db_connection:
            self.db_connection.close()
        if not succesfull:
            self.final_response['body'] = json.dumps(exception_str)
            return self.final_response
        self.final_response['body'] = json.dumps([{'date': self.initial_date_short, 'data': self.df_classified_dict, 'sector':self.assessment_info['SECTOR'], 'archive_id': self.assessment_info['ID_ARCHIVE']}])
        return self.final_response
        
        
def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    
    
def get_especific_error_line():
    _, _, exc_tb = sys.exc_info()
    return str(traceback.extract_tb(exc_tb)[-1][1])
    
    

    

        
@handler_wrapper('Creando directorio','Directorio creado satisfactoriamente','Error en la creacion del directorio','No se pudo crear el directorio')
def create_directory(df_ready):
    df_ready['nivel'] = df_ready['account'].map(len)
    directory = {}
    for cuenta in df_ready.loc[df_ready['nivel'] == 1, 'account']:
        sons = get_sons_of(df_ready,cuenta)
        directory[cuenta] = sons

        for son in sons:
            grand_sons = get_sons_of(df_ready, son)
            directory[son] = grand_sons

    return directory


def get_sons_of(df, cuenta):
    current_level = len(cuenta)
    lower_cat_df = df.loc[(df["nivel"] > current_level) &
                          (df["account"].str.startswith(cuenta))]

    if lower_cat_df.empty:
        return {}

    lower_cat_niveles = list(lower_cat_df["nivel"].unique())
    lower_cat_niveles = sorted(lower_cat_niveles)
    if len(lower_cat_niveles) > 1 and current_level ==1: #este es un bloqueador, como voy a poder abrir solo los ojos de nivel 1 y 2, al abrir el de 2 tiene que mostrarme todas las cuentas que sean hijas de esa cuenta de nivel 2
        lower_cat_df = lower_cat_df.loc[df["nivel"] == lower_cat_niveles[0]]
    return sorted(lower_cat_df['account'].values)
    