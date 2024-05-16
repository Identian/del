import json
import logging
import sys
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import text

from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db, connect_to_db_local_dev

logging.basicConfig()
logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(module)s] %(message)s', force=True, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event,context):
    sc_obj = script_object(event)
    lambda_response = sc_obj.starter()
    logger.info(f'respuesta final de lambda: \n{lambda_response}')
    return lambda_response

######################################
#Candidato para depreciación
######################################
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
            
            self.vectors = ['ITEM_DATE','INITIAL_BALANCE', 'DISBURSEMENT', 'AMORTIZATION', 'ENDING_BALANCE', 'INTEREST_VALUE', 'ENDING_BALANCE_VARIATION']
            
            self.current_debt_items = list()
            self.future_debt_items = list()
            
            self.db_debt_records = dict()
            
            self.current_debt_array = list()
            self.future_debt_array = list()
            self.total_assessment_dates = list()
            
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
            self.get_assessment_dates() #TODO: ver si Dani necesita el maxYear
            self.get_debt_data()
            if self.current_debt_items:
                self.organize_current_debt_items()

            if self.future_debt_items:
                self.organize_future_debt_items()
            #self.delete_previous_results()
            #self.upload_debt_records()
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
        found_dates = [row._asdict() for row in rds_data.fetchall()]
        for date_item in found_dates:
            #logger.info(f'[mira aca]{date_item}')
            self.db_debt_records[date_item['DATES'].strftime('%Y')] = {'DISBURSEMENT': 0, 'INTEREST_VALUE': 0}
            directory.get(date_item['PROPERTY'], []).append(date_item['DATES'].strftime('%Y-%m-%d'))
        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')
    
    
    @handler_wrapper('Adquiriendo datos guardados de deuda', 'Datos de deuda adquiridos con exito', 'Error adquiriendo datos de deuda', 'Error adquiriendo datos')
    def get_debt_data(self):
        query = f"""SELECT ACCOUNT_NUMBER AS account, ALIAS_NAME, ITEM_DATE, INITIAL_BALANCE, DISBURSEMENT, AMORTIZATION, ENDING_BALANCE, INTEREST_VALUE, ENDING_BALANCE_VARIATION
FROM PROJECTED_DEBT WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY ITEM_DATE"""

        logger.info(f"[get_debt_data] Query a base de datos para obtener los datos que existan de deuda:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        directory = {'0': self.future_debt_items}
        for row in [row._asdict() for row in rds_data.fetchall()]:
            logger.info(f'[mira aca]{row}')
            row['INITIAL_BALANCE'] = float(row['INITIAL_BALANCE'])
            row['DISBURSEMENT'] = float(row['DISBURSEMENT'])
            row['AMORTIZATION'] = float(row['AMORTIZATION'])
            row['ENDING_BALANCE'] = float(row['ENDING_BALANCE'])
            row['INTEREST_VALUE'] = float(row['INTEREST_VALUE'])
            row['ENDING_BALANCE_VARIATION'] = float(row['ENDING_BALANCE_VARIATION'])
            #self.integrate_row_to_records(row) #toca sacarlo porque esto es logica de negocio
            
            directory.get(row['account'], self.current_debt_items).append(row)

        self.future_debt_items = [item for item in self.future_debt_items if item['ALIAS_NAME'] != 'Deuda de Tesoreria']
        logger.info(f'[get_debt_data] Deudas re organizadas, resultados:\nDeudas actuales: {self.current_debt_items}\nDeudas futuras: {self.future_debt_items}')
        logger.info(f'[get_debt_data] Records de deudas organizados, resultados:\n{self.db_debt_records}')
        
    
    @handler_wrapper('Organizando objetos de deuda actual', 'Objetos de deuda actual organizados con exito','Error organizando objetos de deuda actual', 'Error organizando objeto de deuda actual')
    def organize_current_debt_items(self):
        found_accounts = sorted(set(row['account'] for row in self.current_debt_items))
        
        for account in found_accounts:
            base_properties = next(item for item in self.current_debt_items if item['account'] == account)
            vector_values = {}
            for vec in self.vectors:
                vector_values[vec] = [item[vec] for item in self.current_debt_items if item['account'] == account][len(self.historic_dates):]

            vector_values['ITEM_DATE'] = [date.strftime('%Y') for date in vector_values['ITEM_DATE']]
            vector_values['ITEM_DATE'][0] = vector_values['ITEM_DATE'][0] if '-12-' in self.historic_dates[-1] else f"Diciembre {vector_values['ITEM_DATE'][0]}"

            self.current_debt_array.append({'name': base_properties['ALIAS_NAME'],
                'years':vector_values['ITEM_DATE'],
                'initialBalance':vector_values['INITIAL_BALANCE'],
                'disbursement': [0] * len(vector_values['ITEM_DATE']),
                'amortization':vector_values['AMORTIZATION'],
                'finalbalance':vector_values['ENDING_BALANCE'],
                'interest':vector_values['INTEREST_VALUE'],
                'finalBalanceVariation':vector_values['ENDING_BALANCE_VARIATION']})
        
        
    @handler_wrapper('Organizando objetos de deuda futura', 'Objetos de deuda futura organizados con exito','Error organizando objetos de deuda futura', 'Error organizando objeto de deuda futura')
    def organize_future_debt_items(self):
        found_aliases = sorted(set(row['ALIAS_NAME'] for row in self.future_debt_items))
        
        for alias in found_aliases:
            base_properties = next(item for item in self.future_debt_items if item['ALIAS_NAME'] == alias)
            vector_values = {}
            for vec in self.vectors:
                vector_values[vec] = [item[vec] for item in self.future_debt_items if item['ALIAS_NAME'] == alias]
            vector_values['ITEM_DATE'] = [date.strftime('%Y') for date in vector_values['ITEM_DATE']]
            self.future_debt_array.append({'name': base_properties['ALIAS_NAME'],
                'years':vector_values['ITEM_DATE'],
                'initialBalance':vector_values['INITIAL_BALANCE'],
                'disbursement':vector_values['DISBURSEMENT'],
                'amortization':vector_values['AMORTIZATION'],
                'finalbalance':vector_values['ENDING_BALANCE'],
                'interest':vector_values['INTEREST_VALUE'],
                'finalBalanceVariation':vector_values['ENDING_BALANCE_VARIATION']})

    @debugger_wrapper('Error integrando proyeccion de deuda a records de bd', 'Error integrando deuda a records de bd')
    def integrate_row_to_records(self, row):
        try:
            logger.warning(f'[mira aca] Integrando:{row} a:\n{self.db_debt_records}')
            if row['YEAR'] in self.historic_dates:
                self.db_debt_records[row['YEAR']]['DISBURSEMENT'] = 0
                self.db_debt_records[row['YEAR']]['INTEREST_VALUE'] = 0
                logger.info(f'row {row} ha sido omitido')
                return
            self.db_debt_records[row['YEAR']]['DISBURSEMENT'] = self.db_debt_records[row['YEAR']]['DISBURSEMENT'] + row['ENDING_BALANCE_VARIATION'] 
            self.db_debt_records[row['YEAR']]['INTEREST_VALUE'] = self.db_debt_records[row['YEAR']]['INTEREST_VALUE'] + row['INTEREST_VALUE']
            
            for date in self.projection_dates:
                if date not in self.db_debt_records:
                    self.db_debt_records[date] = {'DISBURSEMENT': 0, 'INTEREST_VALUE': 0}
        
        except Exception as e:
            logger.error(f'[integrate_row_to_records] Este error no se debió haber activado, pero por si acaso, se estaba intentando integrar el row \n{row}\ncon los records:\n{self.db_debt_records}\nY lanzó el error {str(e)}')
            

    @handler_wrapper('Organizando Respuesta final', 'Respuesta final organizada con exito', 'Error organizando respuesta final', 'Error organizando respuesta final')
    def organize_partial_response(self):
        self.partial_response = {'existingDebt': self.current_debt_array, 'futureDebt': self.future_debt_array}
        
    
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
    event = {"pathParameters": {"id_assessment": "2013"}}
    lambda_handler(event, '')