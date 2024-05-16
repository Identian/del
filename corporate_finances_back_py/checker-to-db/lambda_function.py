
import boto3
import json
import logging
import sys
import time
import urllib
import traceback
from utils import *
import os

import pandas as pd
import numpy as np

from decorators import handler_wrapper, timing

logger = logging.getLogger()
logger.setLevel(logging.INFO)

    
def lambda_handler(event, context):
    """:
    Funcion lambda_handler que se activa automaticamente. La función llama los demás metodos de la lambda
    que contienen la logica necesaria para obtener los resultados. Si en el procedimiento hay errores
    se dispara un raise que ejecuta una respuesta base que debería servir para encontrar la ubicacion
    del error.
    :param event: Se puede interpretar como variables base que activan la lambda
    :param context: contiene informacion relevante al ambiente de la lambda, raramente se usa
    :returns: La funcion debe devolver un objeto con los encabezados y el body en la estructura esperada;
    """
    #logger.info(f'event de entrada: {str(event)}')
    sc_obj = script_object(event)
    return sc_obj.starter()
    
    
class script_object:
    
    def __init__(self, event):
        try:
            
            logger.info('[__INIT__] Inicializando objeto lambda ...')
            logger.info(f'event de entrada: {str(event)}')

            logger.info('[__INIT__] Inicializando objeto lambda ...')
            logger.info(f'event de entrada_ {str(event)}')
            event_body_json = event["body"]
            event_body_dict = json.loads(event_body_json)
            self.raw_dataframe = pd.DataFrame.from_records(event_body_dict['dataframe']) 
            
            try:
                self.found_archive = event_body_dict['archive']['found_archive']
                self.create_archive_info = 0
                
            except Exception:
                self.found_archive = 0
                self.create_archive_info = event_body_dict['archive']
            self.sector = event_body_dict['archive']['sector']
            self.initial_date = event_body_dict['archive']['date']
            self.df_full_original = pd.core.frame.DataFrame()
            self.cleaned_df = pd.core.frame.DataFrame()
            self.ready_to_upload_df = pd.core.frame.DataFrame()
            self.db_connection = 0
            self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'}, 'statusCode': 200}
            self.failed_init = False
            
            logger.info('[__INIT__] Objeto lambda inicializada exitosamente')
            
        except Exception as e:
            self.failed_init = True
            logger.error(f"[__INIT__] error en inicializacion, linea: {get_especific_error_line()}, motivo: "+str(e)) 


    def starter(self):
        try:
            if self.failed_init:
                raise Exception('Falla inicializacion, revisar logs')
            self.df_cleaner()
            self.puc_translator()
            self.level_asign()
            self.master_checker()
            self.create_conection_to_db()
            
            if self.found_archive:
                logger.warning('[starter] Se requiere hace un safe delete de los datos en la tabla de ORIGINAL_PUC')
                self.safe_delete_on_bd()
            
            if self.create_archive_info:
                logger.warning('[starter] Se requiere crear el archive en la tabla de ARCHIVES')
                self.create_archive_on_bd()
                
            self.get_df_ready_to_db()
            self.send_to_db()
            self.db_connection.close()
            return self.response_maker(succesfull = True)
            
        except Exception as e:
            if self.db_connection:
                self.db_connection.close()
            logger.error(f'[starter] Error en el procesamieno del comando de la linea: {get_current_error_line()}, motivo: {e}')
            return self.response_maker(succesfull = False, exception_str = e)
            
    @handler_wrapper('Iniciando alistamiento de dataframe','Dataframe alistado con exito','Error al alistar dataframe','Se presenta error con los datos recibidos')
    def df_cleaner (self):
        self.cleaned_df = self.raw_dataframe.copy()
        self.cleaned_df.fillna('', inplace=True)
        self.cleaned_df = self.cleaned_df.astype({"balance": "string","account":"string","name":"string"}, copy=True)
        self.cleaned_df = self.cleaned_df[self.cleaned_df['account'].apply(lambda x: x.split('.')[0].isnumeric())] #esta linea sirve para que eliminar las filas donde 'account no tiene un valor numerico' o es un string vacio
        self.cleaned_df['balance'] = pd.to_numeric(self.cleaned_df['balance'], errors='coerce')
        self.cleaned_df['account'] = self.cleaned_df['account'].str.split('.').str[0]
        self.cleaned_df.dropna(subset=['balance','account'],inplace=True)
        self.cleaned_df.astype({"balance": "float"}, copy=False)
        self.cleaned_df.sort_values(by=['account'], inplace=True)
        self.cleaned_df.reset_index(drop=True, inplace=True)


    @handler_wrapper('Chequeando si el puc es de cliente o super','Traduccion de puc completada','Error al traducir puc','Hubieron errores en el alistamiento de la informacion')
    def puc_translator(self):
        logger.info('[puc_translator] Revisando si el puc depurado es de tipo superfinanciera...')
        if self.cleaned_df.iloc[0,0] == '100000':
            temp_df = self.cleaned_df.copy()
            logger.info('[puc_translator] El puc depurado es de tipo superFinanciera, traduciendo...')
            for ending in ["00000", "0000", "00"]:
                temp_df.loc[temp_df["account"].str.endswith(ending),"account"] = temp_df.loc[temp_df["account"].str.endswith(ending),"account"].str.slice(stop=(6-len(ending)))
            self.cleaned_df = temp_df.copy()
        else:
            logger.info('[puc_translator] El puc ya es de tipo Default')
            
            
    @handler_wrapper('Asignando niveles a dataframe alistado','Niveles asignados al dataframe','Error asignando los niveles al dataframe','Error revisando los datos recibidos')
    def level_asign(self):
        self.cleaned_df["checked"] = False
        self.cleaned_df['nivel'] = self.cleaned_df['account'].astype(str).str.len()
        self.cleaned_df['checked_balance'] = self.cleaned_df['balance']
        self.cleaned_df['created_data'] = 0

    
    @handler_wrapper('iniciando master checker','Master checker terminado con exito','Error calculando chequeos','Error revisando los datos recibidos')
    def master_checker(self):
        logger.warning(f"Dataframe con el que voy a hacer los chequeos:\n {str(self.cleaned_df.to_dict(orient = 'records'))}")
        self.negative_accounts_check()
        self.subchapter_check()

        if False in self.cleaned_df["checked"].unique(): ##revisar si puedo sacar este unique
            logger.warning("Chequeo de subcapitulos no satisfactorio, agregando cuentas superiores...")
            self.upper_accounts_checker()
        
        self.finance_account_fixer()
        
        self.cleaned_df.sort_values(by=['account'], inplace=True)
        self.cleaned_df.reset_index(drop=True, inplace=True)

    
    @handler_wrapper('Haciendo chequeo de cuentas negativas','Chequeo de cuentas negativas terminado con exito','Error en el chequeo de cuentas','Error revisando capitulos negativos')
    def negative_accounts_check(self):
        account_groups = ["2", "3", "4", "6"]
        for group in account_groups:
            try:
                group_value = self.cleaned_df.loc[self.cleaned_df["account"] == group]["checked_balance"].values[0]
            except Exception as e:
                continue
    
            if group_value < 0:
                self.cleaned_df.loc[self.cleaned_df['account'].str.startswith(group), 'checked_balance'] = self.cleaned_df.loc[self.cleaned_df['account'].str.startswith(group), 'checked_balance'] * -1
                logger.info(f'se realizó cambio de signo para las cuentas del capitulo {group}')
    

    @handler_wrapper('Chequeando subcuentas','Chequeo de subcuentas terminado','Error en el chequeo de subcuentas','Error realizando chequeos preliminares a guardado')
    def subchapter_check(self):
        df = self.cleaned_df.copy()
        niveles = list(df["nivel"].unique())
        niveles = sorted(niveles, reverse=True)
        for nivel in niveles[1:]:
    
            #logger.info("Revisando las cuentas de nivel: "+str(nivel))
            df_nivel = df.loc[df["nivel"] == nivel]
            for upper_cat in df_nivel["account"]:
                lower_cat_df = df.loc[(df["nivel"] > nivel) &
                                      (df["account"].str.startswith(upper_cat)) &
                                      (df["checked"] == False)]
                
                if lower_cat_df.empty:
                    continue
                    
                lower_cat_niveles = list(lower_cat_df["nivel"].unique())
                lower_cat_niveles = sorted(lower_cat_niveles)
                if len(lower_cat_niveles) > 1:
                    lower_cat_df = lower_cat_df.loc[df["nivel"] == lower_cat_niveles[0]]
                upper_cat_value = df.loc[df["account"] == upper_cat]["checked_balance"].values[0]
                lower_cat_sum = lower_cat_df["checked_balance"].sum()
                check_result = checker(upper_cat_value, lower_cat_sum)
                
                df.loc[df['account'].isin(lower_cat_df.account), 'checked'] = check_result
    
            if nivel == 1:
                df.loc[df['nivel'] == 1, 'checked'] = True
       
        self.cleaned_df['checked'] = df['checked']
    
    
    @handler_wrapper('Entrando a creador de cuentas nuevas','Creador de cuentas nuevas terminado con exito','Error en creador de cuentas nuevas','Error chequeando valores recibidos')
    def upper_accounts_checker(self):
        niveles = list(self.cleaned_df["nivel"].unique())
        niveles = sorted(niveles, reverse=True) #Este va disminuyendo [6,4,2,1]
    
        for index, children_level in enumerate(niveles[:-1]):
            df_to_append = self.new_accounts(children_level, niveles[index+1])


    @handler_wrapper('Buscando cuentas nuevas para un nivel dado','Cuentas creadas con exito para uno de los niveles','Error creando cuentas para uno de los niveles','Error procesando nuevas cuentas')
    def new_accounts(self, children_accounts_level, parent_level):

        failed_accounts_df = self.cleaned_df.loc[self.cleaned_df["checked"] == False]  # .tolist()
    
        new_accounts_df = pd.DataFrame(columns=["account", "name", "balance"])
    
        for account in failed_accounts_df.loc[failed_accounts_df["nivel"] == children_accounts_level, "account"].unique():

            if account[:parent_level] in self.cleaned_df["account"].unique():
                #print(f"la cuenta {account[:new_accounts_level]} ya existe, no es necesario crearla")
                pass
            else:
                failed_accounts_sum = failed_accounts_df.loc[
                    (failed_accounts_df["account"].str.startswith(account[:parent_level])) &
                    (failed_accounts_df["nivel"] == children_accounts_level), "balance"].sum()
    
                new_line_df = pd.DataFrame({"account": [account[:parent_level]],
                                            "name": ["cuenta autogenerada para chequeo de capitulos"],
                                            "balance": [failed_accounts_sum],
                                            "checked_balance": [failed_accounts_sum],
                                            "nivel":[parent_level],
                                            "checked":[False],
                                            "created_data":1
                })
                logger.warning(f'cuenta creada: {account[:parent_level]}, por valor de {failed_accounts_sum}')
                self.cleaned_df = pd.concat([self.cleaned_df, new_line_df])
                #self.cleaned_df.sort_values(by=['account'], inplace=True)     #este no importa si lo quito
                self.cleaned_df.reset_index(drop=True, inplace=True)         #este sí lo necesito o da error en el subchapter_check
    
    
    @handler_wrapper('Revisando ecuacion financiera','Ecuacion financiera chequeada con exito','Error revisando los valores de ecuacion financiera','Error revisando valores de ecuacion financiera')
    def finance_account_fixer(self):
        activos = self.cleaned_df.loc[self.cleaned_df["account"] == "1", "checked_balance"].values[0]
        pasivos = self.cleaned_df.loc[self.cleaned_df["account"] == "2", "checked_balance"].values[0]
        patrimonio = self.cleaned_df.loc[self.cleaned_df["account"] == "3", "checked_balance"].values[0]
        ingresos = self.cleaned_df.loc[self.cleaned_df["account"] == "4", "checked_balance"].values[0]
        gastos = self.cleaned_df.loc[self.cleaned_df["account"] == "5", "checked_balance"].values[0]

        logger.warning(f'Los valores encontrados son: \nactivo: {activos}, \npasivo: {pasivos}, \npatrimonio: {patrimonio}, \ningresos: {ingresos}, \ngastos: {gastos}')

        if not checker(activos, pasivos + patrimonio):
            delta_diferencia = activos - pasivos - patrimonio
            logger.warning(f'La ecuacion financiera no chequeó, el delta de activos - pasivos - patrimonio es: {delta_diferencia}')
            delta_diferencia = ingresos - gastos
            logger.warning(f'Delta diferencia de ingresos - gastos: {delta_diferencia}')
    
            account_to_modify_directory = {'Real':'36','Financiero':'39'}
            account_to_modify = account_to_modify_directory[self.sector]
            
            if self.cleaned_df.loc[self.cleaned_df['account'] == account_to_modify].empty:
                #crear cuenta
                new_row = pd.Series({'account':account_to_modify,
                'name':'Cuenta creada para chequeo de ecuacion financiera',
                'balance':0,
                'checked':True,
                'nivel':2,
                'checked_balance':delta_diferencia,
                'created_data':1})
                self.cleaned_df = pd.concat([self.cleaned_df, new_row.to_frame().T])
                self.cleaned_df = self.cleaned_df.astype({'name': 'object', 'balance': 'float64', 'checked': 'bool', 'nivel': 'int64', 'checked_balance': 'float64', 'created_data': 'int64'}) #Esto lo necesito porque la concatenación me caga la subida de los datos
            else:
                #modificar cuenta
                logger.warning(f"Valor original de la cuenta a modificar: \n{self.cleaned_df.loc[self.cleaned_df['account'] == account_to_modify].to_string()}")
                self.cleaned_df.loc[self.cleaned_df['account'] == account_to_modify, 'checked_balance'] = self.cleaned_df.loc[self.cleaned_df['account'] == account_to_modify, 'checked_balance'] + delta_diferencia
                self.cleaned_df.loc[self.cleaned_df['account'] == account_to_modify, 'name'] = '[MODIFICADO POR ECUACION FINANCIERA] '+self.cleaned_df.loc[self.cleaned_df['account'] == account_to_modify, 'name']
            
            logger.warning(f"Valor modificado de la cuenta a modificar: \n{self.cleaned_df.loc[self.cleaned_df['account'] == account_to_modify].to_string()}")
            return
        
        logger.warning(f'La ecuacion financier chequeó, no fue necesario agregar o modificar cuentas')


    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_db(self):
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)
    
    
    @handler_wrapper('Haciendo borrado seguro de puc a reemplazar','Borrado seguro completado','Error al realizar borrado seguro','Se presenta error reemplazando informacion en base de datos')
    def safe_delete_on_bd(self):
        query = f"DELETE FROM ORIGINAL_PUC WHERE ID_ARCHIVE = {self.found_archive}"
        logger.warning(query)
        self.db_connection.execute(query)
    
    @handler_wrapper('Creando archive para los datos recibidos','Archive creado y asignado con exito','Error en la creacion del archive requerido','Se presenta error creando informacion en base de datos')
    def create_archive_on_bd(self):
        query_create_archive = f"INSERT INTO ARCHIVE (ID_COMPANY, INITIAL_DATE, PERIODICITY) VALUES ({self.create_archive_info['company_id']}, \"{self.create_archive_info['date']}\", \"{self.create_archive_info['periodicity']}\")"
        logger.info(f'[create_archive] Query de creacion archive: {query_create_archive}')
        self.db_connection.execute(query_create_archive)
        
        query_get_id_created_archive = f"SELECT ID FROM ARCHIVE WHERE ID_COMPANY={self.create_archive_info['company_id']} AND INITIAL_DATE=\"{self.create_archive_info['date']}\" AND PERIODICITY=\"{self.create_archive_info['periodicity']}\" LIMIT 1"
        logger.warning(f'query para encontrar el id con el que se guardó el archive: {query_get_id_created_archive}')
        rds_data = self.db_connection.execute(query_get_id_created_archive)
        self.found_archive = dict(rds_data.mappings().all()[0])['ID']


    @handler_wrapper('Alistando dataframe para enviar a bd','Dataframe alistado correctamente','Error al alistar dataframe','Error alistando datos para guardado en base de datos')
    def get_df_ready_to_db(self):
        logger.warning(f'[get_df_ready_to_db] todo está llegando correctamente a alistamiento a base de datos')
        logger.warning(f'[get_df_ready_to_db] El dataframe a cargar es:')
        logger.warning(f"\n{self.cleaned_df.to_string()}")
        #logger.warning(self.cleaned_df.head(5).to_string())
        self.ready_to_upload_df = self.cleaned_df.copy()
        self.ready_to_upload_df['ID_ARCHIVE'] = self.found_archive
        self.ready_to_upload_df['ACCOUNT_ORDER'] = self.ready_to_upload_df.index
        self.ready_to_upload_df.drop(['checked' , 'nivel'], axis=1, inplace = True)
        self.ready_to_upload_df.rename(columns={'account': 'ACCOUNT_NUMBER','name': 'ACCOUNT_NAME', 'balance':'ORIGINAL_BALANCE', 'checked_balance':'CHECKED_BALANCE', 'created_data':'CREATED_DATA'}, inplace=True)
        
    
    
    @handler_wrapper('Empezando envío de datos a bd','Envio de datos a bd terminado con exito','Error al subir informacion a bd','Hubieron problemas en la carga de la información a base de datos')
    def send_to_db(self):
        query_insert = self.ready_to_upload_df.to_sql(name = 'ORIGINAL_PUC', con=self.db_connection, if_exists='append', index=False)
    
    
    def response_maker(self, succesfull = False, exception_str = str()):
        if not succesfull:
            self.final_response['body'] = json.dumps({'succesfull':0,'message':exception_str, 'date': self.initial_date})
            return self.final_response
        self.final_response['body'] = json.dumps({'succesfull':1,'message':'ok', 'date': self.initial_date})
        return self.final_response
        
    
def checker(a, b):
    return np.isclose(a, b, atol=0.1, rtol=0.001)  #DIFERENCIA RELATIVA PERMITIDA

def get_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    
    
def get_especific_error_line():
    _, _, exc_tb = sys.exc_info()
    return str(traceback.extract_tb(exc_tb)[-1][1])
    
    

    