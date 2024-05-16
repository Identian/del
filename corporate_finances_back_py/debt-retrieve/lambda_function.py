import json
import logging
import sys
import os

from datetime import datetime
from sqlalchemy import text
from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db, connect_to_db_local_dev

logging.basicConfig()
logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(module)s] %(message)s', force=True, datefmt='%Y-%m-%d %H:%M:%S')
#logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', force=True, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
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

            self.id_assessment = event['pathParameters']['id_assessment']
            self.current_debt_items = list()
            self.future_debt_items = list()
            self.treasure_debt_items = list()
            
            self.current_debt_array = list()
            self.future_debt_array = list()
            
            self.historic_dates = list() #
            self.projection_dates = list() #

            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.create_conection_to_db()
            self.get_assessment_dates() 
            self.get_debt_data()
            if self.current_debt_items:
                self.organize_current_debt_items()

            if self.future_debt_items:
                self.organize_future_debt_items()
                
            if self.treasure_debt_items:
                self.organize_treasure_debt_items()
                
            self.organize_partial_response()

            
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


    @handler_wrapper('Obteniendo las fechas del proceso de valoración', 'Fechas del proceso de valroación obtenidas con exito', 'Error obteniendo las fechas del proceso de valoración', 'Error obteniendo fechas del proceso de valoración')
    def get_assessment_dates(self):
        query = f"SELECT DATES, PROPERTY FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY DATES"
        logger.info(f"[get_assessment_dates] Query a base de datos para obtener las fechas utilizadas en el proces de valoración:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        directory = {'HISTORIC': self.historic_dates, 'PROJECTION': self.projection_dates}
        for date_item in rds_data.fetchall():
            directory.get(date_item.PROPERTY, []).append(date_item.DATES.strftime('%Y-%m-%d'))

        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')
    
    
    @handler_wrapper('Adquiriendo datos guardados de deuda', 'Datos de deuda adquiridos con exito', 'Error adquiriendo datos de deuda', 'Error adquiriendo datos')
    def get_debt_data(self):
        query = """SELECT A.ACCOUNT_NUMBER AS account, A.ALIAS_NAME, A.PROJECTION_TYPE, A.START_YEAR, A.ENDING_YEAR, A.DEBT_COMMENT, A.RATE_COMMENT, A.SPREAD_COMMENT, 
B.RATE_ATRIBUTE, B.SPREAD_ATRIBUTE, B.ITEM_DATE, D.ACCOUNT_NAME FROM DEBT A, PROJECTED_DEBT B, ASSESSMENT C, ASSESSMENT_CHECKED D WHERE A.ID_ASSESSMENT = :id_assessment
AND C.ID = A.ID_ASSESSMENT AND C.ID = B.ID_ASSESSMENT AND C.ID = D.ID_ASSESSMENT AND C.ID_ARCHIVE = D.ID_ARCHIVE AND D.ACCOUNT_NUMBER = A.ACCOUNT_NUMBER
AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER AND A.ALIAS_NAME = B.ALIAS_NAME AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER ORDER BY B.ITEM_DATE"""
        logger.info(f"[get_debt_data] Query a base de datos para obtener los datos de deuda existente:\n {query}")
        rds_data = self.db_connection.execute(text(query), {'id_assessment':self.id_assessment})
        self.current_debt_items = [row._asdict() for row in rds_data.fetchall()]
        
        query = """SELECT A.ACCOUNT_NUMBER AS account, A.ALIAS_NAME, A.PROJECTION_TYPE, A.ORIGINAL_VALUE, A.START_YEAR, A.ENDING_YEAR, A.DEBT_COMMENT, A.RATE_COMMENT, A.SPREAD_COMMENT, 
B.RATE_ATRIBUTE, B.SPREAD_ATRIBUTE, B.ITEM_DATE FROM DEBT A, PROJECTED_DEBT B WHERE A.ID_ASSESSMENT = B.ID_ASSESSMENT AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER AND A.ALIAS_NAME = B.ALIAS_NAME 
AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER AND A.ID_ASSESSMENT = :id_assessment AND A.ACCOUNT_NUMBER = 0 ORDER BY B.ITEM_DATE"""
        logger.info(f"[get_debt_data] Query a base de datos para obtener los datos de deuda futura:\n {query}")
        rds_data = self.db_connection.execute(text(query), {'id_assessment':self.id_assessment})
        self.future_debt_items = [row._asdict() for row in rds_data.fetchall()]
        
        self.treasure_debt_items = [item for item in self.future_debt_items if item['ALIAS_NAME'] == 'Deuda de Tesoreria']
        self.future_debt_items = [item for item in self.future_debt_items if item['ALIAS_NAME'] != 'Deuda de Tesoreria']
        logger.info(f'[get_debt_data] Deudas re organizadas, resultados:\nDeudas actuales: {self.current_debt_items}\nDeudas futuras: {self.future_debt_items}\nDeudas de tesorería:{self.treasure_debt_items}')


    @handler_wrapper('Organizando objetos de deuda actual', 'Objetos de deuda actual organizados con exito','Error organizando objetos de deuda actual', 'Error organizando objeto de deuda actual')
    def organize_current_debt_items(self):
        found_accounts = sorted(set(row['account'] for row in self.current_debt_items))
        for account in found_accounts:
            base_properties = next(item for item in self.current_debt_items if item['account'] == account)
            rate_atributtes = [float(item['RATE_ATRIBUTE']) for item in self.current_debt_items if item['account'] == account][len(self.historic_dates):]
            spread_atributtes = [float(item['SPREAD_ATRIBUTE']) for item in self.current_debt_items if item['account'] == account][len(self.historic_dates):]
            years_vector = [item['ITEM_DATE'].strftime('%Y') for item in self.current_debt_items if item['account'] == account][len(self.historic_dates):]
            years_vector[0] = years_vector[0] if '-12-' in self.historic_dates[-1] else f"Diciembre {years_vector[0]}"
            
            self.current_debt_array.append({'accountNumber':account,
                'expiration':base_properties['ENDING_YEAR'],
                'accountName':base_properties['ACCOUNT_NAME'],
                'name':base_properties['ALIAS_NAME'],
                'method':base_properties['PROJECTION_TYPE'],
                'explication':base_properties['DEBT_COMMENT'],
                'years':years_vector,
                'rates':{'values':rate_atributtes, 'explication':base_properties['RATE_COMMENT']},
                'spread':{'values':spread_atributtes, 'explication':base_properties['SPREAD_COMMENT']}
            })
        

    @handler_wrapper('Organizando objetos de deuda futura', 'Objetos de deuda futura organizados con exito','Error organizando objetos de deuda futura', 'Error organizando objeto de deuda futura')
    def organize_future_debt_items(self):
        found_aliases = sorted(set(row['ALIAS_NAME'] for row in self.future_debt_items))
        for alias in found_aliases:
            base_properties = next(item for item in self.future_debt_items if item['ALIAS_NAME'] == alias)
            rate_atributtes = [float(item['RATE_ATRIBUTE']) for item in self.future_debt_items if item['ALIAS_NAME'] == alias]
            spread_atributtes = [float(item['SPREAD_ATRIBUTE']) for item in self.future_debt_items if item['ALIAS_NAME'] == alias]
            years_vector = [item['ITEM_DATE'].strftime('%Y') for item in self.future_debt_items if item['ALIAS_NAME'] == alias]
            
            self.future_debt_array.append({'newAmount': float(base_properties['ORIGINAL_VALUE']),
                'disburmentYear':base_properties['START_YEAR'],
                'finalYear':base_properties['ENDING_YEAR'],
                'name':base_properties['ALIAS_NAME'],
                'method':base_properties['PROJECTION_TYPE'],
                'explication':base_properties['DEBT_COMMENT'],
                'years':years_vector,
                'rates':{'values':rate_atributtes, 'explication':base_properties['RATE_COMMENT']},
                'spread':{'values':spread_atributtes, 'explication':base_properties['SPREAD_COMMENT']}
            })
    
    @handler_wrapper('Se encontraron tasas de deuda de tesorería, organizando', 'Deuda de tesorería organizada con exito', 'Error organizando deuda de tesorería', 'Error organizando tasas de tesorería')
    def organize_treasure_debt_items(self):
        base_properties = self.treasure_debt_items[0]
        self.treasure_debt_record = {'disburmentYear':base_properties['START_YEAR'], 
                                'finalYear': base_properties['ENDING_YEAR'], 
                                'name': 'Deuda de Tesoreria', 
                                'method': 'Amortización lineal', 
                                'explication': base_properties['DEBT_COMMENT'],
                                'years':[],
                                'rates':{'values':[],'explication':base_properties['RATE_COMMENT']},
                                'spread':{'values':[],'explication':base_properties['SPREAD_COMMENT']}}

        for item in self.treasure_debt_items:
            self.treasure_debt_record['years'].append(item['ITEM_DATE'].strftime('%Y'))
            self.treasure_debt_record['rates']['values'].append(safe_exit(item['RATE_ATRIBUTE']))
            self.treasure_debt_record['spread']['values'].append(safe_exit(item['SPREAD_ATRIBUTE']))
    
    
    
    @handler_wrapper('Organizando Respuesta final', 'Respuesta final organizada con exito', 'Error organizando respuesta final', 'Error organizando respuesta final')
    def organize_partial_response(self):
        min_year = int(self.projection_dates[1].split('-')[0]) if '-12-' in self.projection_dates[0] else int(self.projection_dates[0].split('-')[0]) 
        self.partial_response = {'minYear':min_year, 'maxYear':int(self.projection_dates[-1].split('-')[0]), 'financialDebt': self.current_debt_array, 'futureDebt': self.future_debt_array, 'treasureDebt': []}
        if self.treasure_debt_items:
            self.partial_response['treasureDebt'] = [self.treasure_debt_record]

    
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
    
def safe_exit(j):
    try:
        return float(j)
    except:
        return 0
    

if __name__ == "__main__":
    event = {"pathParameters": {"id_assessment": "47"}}
    lambda_handler(event, '')