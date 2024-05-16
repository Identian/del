from sqlalchemy import text
import datetime
import json
import logging
import os
import sys
import sqlalchemy
import traceback
import pandas as pd

from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db, connect_to_db_local_dev
from _full_recurrence import recurrence_class
from _full_dynamic import dynamic_class
from _recalculating_methods import recalculating_methods_class
from _orchester_utils import orchester_utils_class

logging.basicConfig()
logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(module)s] %(message)s', force=True, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    sc_obj = script_object(event)
    lambda_response = sc_obj.starter()
    logger.info(f'respuesta final de lambda: \n{lambda_response}')
    return lambda_response
    
    
class script_object(recurrence_class, recalculating_methods_class, orchester_utils_class, dynamic_class):

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
            self.id_assessment = 0
    
            logger.warning(f'event de entrada: {str(event)}')
            event_dict = event.get('body', False)

            if event_dict:
                event_dict = json.loads(event_dict)     
                self.calculate_recurrence = event_dict['recurrence'] #Esta llave la manda el front para ver si crea el id_assessment y ejecuta (o no) recurrencia
                self.nit = event_dict['nit']
                self.current_short_date = event_dict['date']
                self.current_date_dt = datetime.datetime.strptime(self.current_short_date, "%d-%m-%Y")
                self.current_long_date = self.current_date_dt.strftime('%Y-%m-%d %H:%M:%S') #este tenerlo en cuenta, creo que de todas formas no se está usando porque ya se usa self.assessment_initial_date
                self.historic_dates_chosen = [event_dict['date']] + event_dict['selected_dates']
                self.historic_dates_chosen = ['-'.join(date.split('-')[::-1]) for date in self.historic_dates_chosen] #acá estoy girando las fechas de entrada para que sean %Y-%m-d
                self.current_periodicity = event_dict['periodicity']
                self.user = event_dict['user']
                self.context = 'full_recurrency'
            
            
            else:
                self.id_assessment = event['id_assessment']
                self.context = 'dynamic_calc'

            self.context_methods = {
                'Depreciación de activos existentes'    : self.fixed_assets_recalc,
                'Deuda financiera'                      : self.debt_recalc,
                'Pyg A'                                 : self.pyg_first_half_recalc,
                'Proyecciones pyg A'                    : self.pyg_first_half_projections_recalc,
                'D&A Capex'                             : self.new_capex_recalc,
                'Summary capex'                         : self.capex_summary_recalc,
                'Pyg B'                                 : self.pyg_second_half_recalc,
                'Proyecciones pyg B'                    : self.pyg_final_projections_recalc,
                'Pyg'                                   : self.final_pyg,
                'Patrimonio'                            : self.patrimony_recalc,
                'Capital de trabajo'                    : self.working_capital_recalc,
                'Otras proyecciones'                    : self.other_projections_recalc,
                'Flujo de caja'                         : self.cash_flow_recalc,
                'Valoración'                            : self.assessment_recalc
            }
            
            self.current_context = 0

            self.historic_dates = list()
            self.historic_dates_len = int()
            self.historic_dates_long = list()

            self.projection_dates = list()
            self.projection_dates_len = int()
            self.projection_dates_long = list()
            self.new_assessment_date = str()
            self.all_dates_long = list()
            self.all_dates_len = int()

            self.raw_classification = list()
            self.raw_pyg = list()
            self.raw_cash_flow = list()

            self.fixed_assets_data_to_new_capex = list()
            self.fixed_assets_data_to_capex_summary =dict()
            self.fixed_assets_proy_to_capex_summary = list()
            self.raw_cash_flow = list()
            self.assessment_projections_found = False

            self.noting_list = list()
            self.steps_list = list()
            self.capex_summary_vectors = dict()
            self.coupled_debt_records = list()
            self.pyg_results_records = list()
            self.patrimony_results_records = list()
            
            self.capex_summary_records = list()
            self.wk_results_records = list()
            self.op_results_records = list()
            self.dep_capex = list()
            self.assessment_models = dict()
            self.new_assessment_historic_archives = list()
            self.pyg_values_vectors = dict()
            self.pyg_hints_vectors = dict()
            self.current_pyg_totals = dict()
            self.new_capex_vector = list()
            self.operation_income_vector = list()
            self.new_assessment_has_annual = False

        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True


    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
                
            logger.info('[starter] Empezando starter de objeto lambda')
            self.create_conection_to_db()
            self.get_raw_classification()
            self.get_raw_pyg()
            self.get_raw_cash_flow()
            if self.context == 'full_recurrency':
                self.full_recurrence_starter()
                if not self.calculate_recurrence:
                    return self.response_maker(succesfull_run = True)
            
            if self.context == 'dynamic_calc':
                self.full_dynamic_starter()
            
            self.organize_all_dates_info()
            self.orchestrator_state_update('WORKING')
            self.recalculating_starter()
            #self.db_connection.commit()
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


    def get_raw_classification(self):
        query = """SELECT * FROM RAW_CLASSIFICATION"""
        logger.info(f"[get_raw_classification] Query a base de datos para obtener el raw de clasificaciones:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        self.raw_classification = [row._asdict() for row in rds_data.fetchall()]
        self.easy_classification_dict = {item['ID']:item['CLASSIFICATION'] for item in self.raw_classification}
        self.classification_id_dict = {item['CLASSIFICATION']: item['ID'] for item in self.raw_classification}


    def get_raw_pyg(self):
        query = """SELECT * FROM RAW_PYG"""
        logger.info(f"[get_raw_pyg] Query a base de datos para obtener el raw de pyg:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        self.raw_pyg = [row._asdict() for row in rds_data.fetchall()]
        self.easy_raw_pyg_dict = {item['PYG_ITEM_NAME']:item['ID'] for item in self.raw_pyg}
        self.id_raw_pyg_dict = {item['ID']:item['PYG_ITEM_NAME'] for item in self.raw_pyg}

    def get_raw_cash_flow(self):
        query = """SELECT * FROM RAW_CASH_FLOW"""
        logger.info(f"[get_raw_cash_flow] Query a base de datos para obtener el raw de cash_flow:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        self.raw_cash_flow = [row._asdict() for row in rds_data.fetchall()]
        self.cash_flow_id_dict = {item['CASH_FLOW_ITEM_NAME']: item['ID'] for item in self.raw_cash_flow}

    
    @handler_wrapper('Organizando las formas de fechas que se van a utilizar', 'Fechas del proceso de valoración organizadas con exito', 'Error organizando fechas dle proceso de valoración', 'Error organizando fechas')
    def organize_all_dates_info(self):
        self.new_assessment_date = self.historic_dates[-1] #esta variable se usa solo una vez en pyg, revisar si hay otra forma de usarla allá para eliminarla acá
        self.historic_dates_len = len(self.historic_dates)
        self.projection_dates_len = len(self.projection_dates)
        
        logger.warning(f'[mira aca] esto debería estar en formato fecha: {self.historic_dates}')
        self.historic_dates_long = [date.strftime('%Y-%m-%d %H:%M:%S') for date in self.historic_dates]
        self.assessment_initial_date = self.historic_dates_long[-1]
        self.projection_dates_long = [datetime.datetime.strptime(str(date), '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S') for date in self.projection_dates]
        self.all_dates_long = self.historic_dates_long + self.projection_dates_long
        self.all_dates_len = len(self.all_dates_long)

    
    @debugger_wrapper('Error construyendo respuesta final', 'Error construyendo respuesta')
    def response_maker(self, succesfull_run = False, error_str = str()):
        if succesfull_run:
            self.final_response['statusCode'] = 200
            self.final_response['body'] = json.dumps({"id_assessment": self.id_assessment})
            self.orchestrator_state_update('SUCCES')
            self.db_connection.commit() if __name__ != 'lambda_function' else None
        else:
            self.orchestrator_state_update('ERROR')
            self.final_response['body'] = json.dumps(self.detailed_raise if self.detailed_raise else error_str)
            self.db_connection.rollback() if __name__ != 'lambda_function' else None
        if self.db_connection:
                self.df_and_upload(self.noting_list, 'NOTES')
                self.db_connection.close()
        return self.final_response

        

def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)


if __name__ == "__main__":
    #event = {'body': "{\"date\":\"18-08-2023\",\"nit\":\"262626262-1\",\"periodicity\":\"Anual\",\"user\":\"dmanchola@precia.co\",\"recurrence\":false, \"selected_dates\": []}"}
    event = {"id_assessment": 2071}
    lambda_handler(event, '')

