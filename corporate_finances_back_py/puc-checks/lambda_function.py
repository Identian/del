""":
capas:
capa-pandas-data-transfer

variables de entorno
ARCHIVES_TABLE : ARCHIVE
COMPANIES_TABLE : COMPANY
DB_SCHEMA : src_corporate_finance
PUCS_TABLE : ORIGINAL_PUC
SECRET_DB_NAME : precia/rds8/sources/finanzas_corporativas
SECRET_DB_REGION : us-east-1

RAM: 1024
"""

import json
import logging
import sys
import datetime

import pandas as pd
import numpy as np
import os
from decorators import handler_wrapper, timing
from utils import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info(f'[lambda_handler] event de entrada:\n{event}')
    sc_obj = script_object(event)
    return sc_obj.starter()

class script_object():

    def __init__(self, event) -> None:
        try:
            logger.info('[__INIT__] Inicializando Lambda ...')
            
            self.companies_table = os.environ['COMPANIES_TABLE']
            self.archives_table = os.environ['ARCHIVES_TABLE']
            self.puc_table = os.environ['PUCS_TABLE']

            event_body_dict = json.loads(event["body"])
            self.nit = event_body_dict["nit"]
            self.initial_date_short = event_body_dict["date"]
            self.initial_date_long = datetime.datetime.strptime(self.initial_date_short, "%d-%m-%Y").strftime('%Y-%m-%d %H:%M:%S')
            self.periodicity = event_body_dict['periodicity']

            self.db_connection = 0

            self.company_info = dict()
            self.archive_info = dict()

            self.raw_puc_df = pd.core.frame.DataFrame()
            self.df_ready = pd.core.frame.DataFrame()

            self.results_response = {}
            
            self.failed_init = False
            self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'}}
            logger.info('[__INIT__] Lambda inicializada exitosamente')

        except Exception as e:
            self.failed_init = True
            logger.error(f"[__INIT__] error en inicializacion, motivo: " + str(e))


    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Falla inicializacion, revisar logs')

            self.create_db_conection()
            self.get_company_info()
            self.get_archive_info()
            self.get_puc_data()
            self.setting_up_df()
            self.master_checker()

            return self.response_maker(succesfull = True)     
        except Exception as e:
            logger.error(f'[starter] Falla en el objeto lambda, linea: {get_error_line()}, \nmotivo: {str(e)}')
            return self.response_maker(succesfull = False, exception_str = str(e))
    
    
    @handler_wrapper('Creando conexion a base de datos','Conexion a base de datos creada con exito','Error creando conexion a base de datos','Problemas al conectarse a bse de datos')
    def create_db_conection(self):
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)
        #self.db_connection = connect_to_db_local_dev()


    @handler_wrapper('Buscando informacion del nit recibido','La información de la empresa fue encontrada','Error en la busqueda de los datos de la empresa','Error, problemas localizando la informacion de la empresa')
    def get_company_info(self):
        query = f"SELECT * FROM {self.companies_table} WHERE NIT=\"{self.nit}\" LIMIT 1"
        logger.info(f'[get_company_info] query para obtener la informacion de la empresa: {query}')
        rds_data = self.db_connection.execute(query)
        self.company_info = dict(rds_data.mappings().all()[0])
 

    @handler_wrapper('Buscando información del archive solicitado','Información encontrada con exito', 'Error buscando información del archive', 'Error fatal, no se encontró la información solicitada')
    def get_archive_info(self):
        query = f"SELECT * FROM {self.archives_table} WHERE ID_COMPANY={self.company_info['ID']} AND INITIAL_DATE = \"{self.initial_date_long}\" AND PERIODICITY=\"{self.periodicity}\" LIMIT 1"
        logger.info(f"[get_archive_info] Query a base de datos {query}")
        rds_data = self.db_connection.execute(query)        
        self.archive_info = dict(rds_data.mappings().all()[0])


    @handler_wrapper('Obteniendo la informacion chequeada del puc','Informacion de puc encontrada', 'Error al hacer query de los datos chequeados','Error al buscar los datos de puc')
    def get_puc_data(self):
        query = f"SELECT ACCOUNT_NUMBER, ORIGINAL_BALANCE, CHECKED_BALANCE, ACCOUNT_NAME, CREATED_DATA FROM {self.puc_table} WHERE ID_ARCHIVE = {self.archive_info['ID']} ORDER BY ACCOUNT_NUMBER"
        logger.info(f"[get_checked_data_info] Query a base de datos {query}")
        self.raw_puc_df = pd.read_sql(query, self.db_connection);


    @handler_wrapper('Cambiando tipos de datos de dataframe','Dataframe listo para chequeos','Error al alistar Dataframe','Error fatal, comuniquese con Tecnologia')
    def setting_up_df(self):
        logger.info(f'[setting_up_df] columnas que llegan a alistamiento: {self.raw_puc_df.columns}')
        self.raw_puc_df.rename(columns={'ACCOUNT_NUMBER': 'account', 'ACCOUNT_NAME': 'name', 'ORIGINAL_BALANCE':"balance"}, inplace=True)
        self.df_ready = self.raw_puc_df.astype({"account": "string", "name": "string", "balance": "float"}, copy=True)


    @handler_wrapper('Iniciando master_checker','Master checker terminado con exito','Error realizando chequeos','Error realizando chequeos de puc')
    def master_checker(self):
        self.revert_negative_chapters()
        self.level_asign()
        self.subchapter_check('first')
        self.created_accounts()
        self.subchapter_check('final')
        self.finance_math()
        self.finantial_equation_balancer()

        self.results_response["data"] = self.df_ready.to_json(index=False, orient="table") 
        
        
    @handler_wrapper('Empezando chequeo de capitulos negativos','Chequeo de capitulos negativos terminado correctamente','Error durante el chequeo de capitulos negativos','Error durante el chequeo de capitulos negativos')
    def revert_negative_chapters(self):
        account_groups = ["2", "3", "4", "6"]
        report_message = []
        for group in account_groups:
            try:
                group_value = self.df_ready.loc[self.df_ready["account"] == group]["balance"].values[0]
            except Exception:
                continue
            if group_value < 0:
                report_message.append(f"Se ha realizado un cambio de signo en la cuenta {group} y sus subcuentas")

        self.results_response["NegativeAccountInfo"] = report_message


    @handler_wrapper('Asignando niveles a dataframe','Niveles asignados a dataframe','Error al asignar niveles a dataframe','Error al procesor los datos a chequear')
    def level_asign(self):
        if 'nivel' not in self.df_ready.columns:
            self.df_ready["checked"] = False
            self.df_ready['nivel'] = self.df_ready['account'].str.len()


    @handler_wrapper('Iniciando primer chequeo de subcapitulos','terminado primer chequeo de subcuentas','','')
    def subchapter_check(self, context):
        niveles = list(self.df_ready["nivel"].unique())
        niveles = sorted(niveles, reverse=True)
        balance_directory = {'first':'balance', 'final':'CHECKED_BALANCE'}
        balance_to_use = balance_directory[context]

        for nivel in niveles[1:]:
            #logger.info("Revisando las cuentas de nivel: "+str(nivel))
            level_accounts = self.df_ready.loc[self.df_ready["nivel"] == nivel, 'account'].tolist()
            for upper_cat in level_accounts:

                lower_cat_df = self.df_ready.loc[(self.df_ready["nivel"] > nivel) &
                                        (self.df_ready["account"].str.startswith(upper_cat)) &
                                        (self.df_ready["checked"] == False)]
                
                if lower_cat_df.empty:
                    continue
                    
                lower_cat_df = lower_cat_df.loc[lower_cat_df['nivel'] == lower_cat_df['nivel'].min()]

                upper_cat_value = self.df_ready.loc[self.df_ready["account"] == upper_cat, balance_to_use].values[0]
                lower_cat_sum = lower_cat_df[balance_to_use].sum()
                
                self.df_ready.loc[self.df_ready['account'].isin(lower_cat_df.account), 'checked'] = checker(upper_cat_value, lower_cat_sum)
                
            if nivel == 1:
                self.df_ready.loc[self.df_ready['nivel'] == 1, 'checked'] = True

        result_directory = {'first':'SubChapterCheckInfo', 'final':'AfterLoopFailedAccounts'}

        self.results_response[result_directory[context]] = self.df_ready.loc[self.df_ready["checked"] == False, ["account","name","balance"]].to_json(index=False, orient="table")


    @handler_wrapper('Buscando las cuentas que hayan sido creadas','Busqueda terminada y asignada a resultados','Error buscando las cuentas creadas para chequeo de sub cuentas','Error revisando chequeo de subcuentas')
    def created_accounts(self):
        self.results_response['NewAccountsInfo'] = self.df_ready.loc[self.df_ready['CREATED_DATA'] == 1, ["account","name","balance"]].to_json(index=False, orient="table")


    @handler_wrapper('Empezando chequeo de finanzas','Chequeo de finanzas terminado con exito','Error durante el chequeo de finanzas','Error durante el chequeo de finanzas')
    def finance_math(self):

        logger.warning(f'[finance_math] dataframe entrando a chequeo de finanzas: {self.df_ready.head(3).to_string()}')
        """
        activos = self.df_ready.loc[self.df_ready["account"] == "1", "balance"].values[0]
        pasivos = self.df_ready.loc[self.df_ready["account"] == "2", "balance"].values[0]
        patrimonio = self.df_ready.loc[self.df_ready["account"] == "3", "balance"].values[0]
        ingresos = self.df_ready.loc[self.df_ready["account"] == "4", "balance"].values[0]
        gastos = self.df_ready.loc[self.df_ready["account"] == "5", "balance"].values[0]
        """
        self.activos, self.pasivos, self.patrimonio, self.ingresos, self.gastos = self.df_ready.loc[
            (self.df_ready["account"] == "1") | 
            (self.df_ready["account"] == "2") | 
            (self.df_ready["account"] == "3") | 
            (self.df_ready["account"] == "4") | 
            (self.df_ready["account"] == "5") , "CHECKED_BALANCE"].values.tolist()

        try:
            ganancias_perdidas = self.df_ready.loc[self.df_ready["account"] == "59", "CHECKED_BALANCE"].values[0]
        except Exception:
            ganancias_perdidas = 0

        logger.info(f'las propiedades son: \n Activos {self.activos}, \n Pasivos {self.pasivos}, \n Patrimonio {self.patrimonio}, \n Ingresos {self.ingresos}, \n Gastos {self.gastos}')
        check_finance_list = []
        check_finance_list.append(checker(self.activos, self.pasivos + self.patrimonio))
        check_finance_list.append(checker(self.activos, self.pasivos + self.patrimonio + self.ingresos - self.gastos))
        check_finance_list.append(checker(self.activos, self.pasivos + self.patrimonio + self.ingresos - self.gastos + ganancias_perdidas))
        logger.info(f'[finance_math] Resultados financieros: {str(check_finance_list)}')
        self.results_response['FinanceResult'] = [int(item) for item in check_finance_list]

        
    @handler_wrapper('Revisando igualdad de ecuacion financiera','Ecuacion financiera analizada','Error al analizar la ecuacion financiera','Error al incorporar la ecuacion financiera')
    def finantial_equation_balancer(self):
        delta_value = self.ingresos - self.gastos
        d_temp = {'activos': self.activos, 'pasivos':self.pasivos, 'patrimonio':self.patrimonio, 'ingresos':self.ingresos, 'gastos':self.gastos,'delta_value' : delta_value}
        
        for key, value in d_temp.items():
            value = "{0:,.2f}".format(value)
            value = value.replace('.','/')
            value = value.replace(',','.')
            d_temp[key] = value.replace('/',',')
        
        if self.results_response['FinanceResult'][0]:
            self.results_response['EquationBalancer'] = f"La ecuacion financiera primaria no requirió modificacion o creacion de datos: [] Activo: {d_temp['activos']} [] Pasivo: {d_temp['pasivos']} [] Patrimonio: {d_temp['patrimonio']}"
            logger.warning(f"respuesta de balanceador: \n {self.results_response['EquationBalancer']}")
            return
        
        account_to_modify_directory = {'Real':'36','Financiero':'39'}
        account_to_modify = account_to_modify_directory[self.company_info['SECTOR']]

        if not self.df_ready.loc[self.df_ready['name'].str.startswith('[MODIFICADO POR ECUACION FINANCIERA] ')].empty:
            self.results_response['EquationBalancer'] = f"La ecuacion financiera primaria no estaba balanceada, se tuvo que modificar la cuenta {account_to_modify} por un valor de {d_temp['delta_value']} [] Ingresos: {d_temp['ingresos']} [] Gastos: {d_temp['gastos']}"
        else:
            self.results_response['EquationBalancer'] = f"La ecuacion financiera primaria no estaba balanceada, se tuvo que crear la cuenta {account_to_modify} por un valor de {d_temp['delta_value']} [] Ingresos: {d_temp['ingresos']} [] Gastos: {d_temp['gastos']}"
        logger.warning(f"respuesta de balanceador: \n {self.results_response['EquationBalancer']}")

    def response_maker(self, succesfull = False, exception_str = str()):
        if not succesfull:
            self.final_response['body'] = json.dumps(exception_str)
            return self.final_response
        self.final_response['body'] = json.dumps(self.results_response)
        return self.final_response


def checker(a, b):
    return np.isclose(a, b, atol=0.1, rtol=0.001)  #DIFERENCIA RELATIVA PERMITIDA


def get_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
