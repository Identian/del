import json
import logging
import sys
import os
import pandas as pd
import datetime
import decimal
from sqlalchemy import text

print(f'En principal{__name__}')
if __name__ in ['__main__', 'lambda_function']:
    from decorators import handler_wrapper, timing, debugger_wrapper
    from utils import get_secret, connect_to_db, connect_to_db_local_dev
else:
    from .decorators import handler_wrapper, timing, debugger_wrapper
    from .utils import get_secret, connect_to_db, connect_to_db_local_dev

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

            self.id_assessment = event['pathParameters']['id_assessment']
            
            self.not_operational_accounts = list()
            self.debt_accounts = list()
            
            self.cash_flow_dict = ['Flujo de caja Libre Operacional', 'Flujo de caja del accionista', 'Flujo de caja del periodo']
            self.assessment_directory = {'ASSESSMENT_DATE': 'dateAssessment',
                                            'INITIAL_DATE': 'dateInitial',
                                            'CURRENT_CLOSING_DATE': 'dateCurrentClosing',
                                            'FLOW_HALF_PERIOD': 'flowHalfPeriod',
                                            'NEXT_FLOW_HALF_YEAR': 'nextFlowHalfYear',
                                            'DATES_ADJUST_ATRIBUTE': 'adjust',
                                            'DATES_ADJUST_COMMENT': 'explicationDateAdjust',
                                            'CHOSEN_FLOW_NAME': 'cashFree',
                                            'NORMALIZED_CASH_FLOW': 'normalicedCashFlow',
                                            'DISCOUNT_RATE_ATRIBUTE': 'discountRateAtribute',
                                            'TERMINAL_VALUE': 'terminalValue',
                                            'DISCOUNT_FACTOR': 'discountFactor',
                                            'VP_TERMINAL_VALUE': 'vpTerminalValue',
                                            'ENTERPRISE_VALUE': 'enterpriseValue',
                                            'FINANCIAL_ADJUST': 'financialAdjust',
                                            'TOTAL_NOT_OPERATIONAL_ASSETS': 'activesNoOperational',
                                            'TOTAL_OPERATIONAL_PASIVES': 'pasiveOperational',
                                            'PATRIMONY_VALUE': 'patrimonial',
                                            'OUTSTANDING_SHARES': 'nActions',
                                            'ASSESSMENT_VALUE': 'valuePerValuation',
                                            'VP_FLOWS': 'vpFlows',
                                            'GRADIENT': 'gradient',
                                            'ADJUST_METHOD': 'method'
                    
            }
            self.fclo_directory = {'OPERATIVE_CASH_FLOW': 'operativeCashFlow',
                                    'DISCOUNT_PERIOD': 'discountPeriod',
                                    'DISCOUNT_RATE': 'discountRate',
                                    'DISCOUNT_FACTOR': 'discountFactor',
                                    'FCLO': 'fclo'
            }
            
            self.historic_dates = list() #
            self.projection_dates = list() #
            self.cash_flow_response = list()
            self.found_accounts = list()
            self.assessment_atributes = dict()
            self.flow_discount = list()
            self.assessment = dict()

            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.create_conection_to_db()
            self.get_assessment_dates()
            self.acquire_classification_values()
            self.acquire_cash_flows()
            self.check_assessment_atributes()
            if self.assessment_atributes:
                self.acquire_fclo_atributes()
                self.consume_fclo_atributes()
                self.consume_assessment_atributes()

            
            self.organize_partial_response()

            
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
        found_dates = [row._asdict() for row in rds_data.fetchall()]
        for date_item in found_dates:
            directory.get(date_item['PROPERTY'], []).append(date_item['DATES'].strftime('%d-%m-%Y'))
        self.projection_dates = [item.split('-')[-1] for item in self.projection_dates]
        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')
    

    @handler_wrapper('Oteniendo las cuentas de valoración historica', 'Valores de cuentas obtenidos con exito', 'Error obteniendo valores de cuentas', 'Error obtenino valores de cuentas clasificadas')
    def acquire_classification_values(self):
        query = f"""SELECT B.CLASSIFICATION AS clasification, A.ACCOUNT_NAME AS name, A.ANNUALIZED AS value FROM ASSESSMENT_CHECKED A, RAW_CLASSIFICATION B, ASSESSMENT C
WHERE A.ID_ASSESSMENT = C.ID AND A.ID_RAW_CLASSIFICATION = B.ID AND A.ID_ARCHIVE = C.ID_ARCHIVE 
AND B.CLASSIFICATION IN ('Efectivo y no operacionales', 'Caja', 'Deuda con costo financiero') AND A.ID_ASSESSMENT = {self.id_assessment}"""
        logger.info(f"[acquire_classification_values] Query a base de datos para obtener los valores de cuentas con las clasificaciones requeridas:\n{query}")
        rds_data = self.db_connection.execute(text(query))
        self.found_accounts = [row._asdict() for row in rds_data.fetchall()]
        clasification_directory = {'Efectivo y no operacionales': self.not_operational_accounts, 'Deuda con costo financiero': self.debt_accounts}
        for item in self.found_accounts:
            item['value'] = float(item['value'])
            clasification_directory.get(item['clasification'], []).append(item)
        
    
    @handler_wrapper('Obteniendo cajas de flujo de caja', 'Cajas de flujo de caja obtenidas con exito', 'Error adquiriendo resultados de flujo de caja', 'Error adquiriendo resultados de flujo de caja')
    def acquire_cash_flows(self):
        query = f"""SELECT CASH_FLOW_ITEM_NAME, VALUE FROM CASH_FLOW A, RAW_CASH_FLOW B WHERE A.ID_RAW_CASH_FLOW = B.ID AND A.ID_ASSESSMENT = {self.id_assessment} 
AND B.CASH_FLOW_ITEM_NAME IN ('Flujo de Caja Libre Operacional', 'Flujo de Caja del accionista', 'Flujo de Caja del periodo') ORDER BY A.SUMMARY_DATE"""
        logger.info(f"[acquire_cash_flows] Query a base de datos para obtener los valores de flujos de caja requeridos:\n{query}")
        rds_data = self.db_connection.execute(text(query))
        cash_flows = [row._asdict() for row in rds_data.fetchall()]
        for item in cash_flows:
            item['VALUE'] = float(item['VALUE'])
        
        for cash_flow in self.cash_flow_dict:
            values_vector = [row['VALUE'] for row in cash_flows if row['CASH_FLOW_ITEM_NAME'] == cash_flow][-1-len(self.projection_dates):]
            
            values_vector[0] = values_vector[0] if '-12-' in self.historic_dates[-1] else values_vector[1] - values_vector.pop(0)
            self.cash_flow_response.append({'name': cash_flow, 'values': values_vector})

    
    @handler_wrapper('Chequeando si existen atributos de pantalla de valoración', 'Chequeo terminado', 'Error chequeando si hay atributos de pantalla de valoración', 'Error chequeando atributos de pantalla de valoración')
    def check_assessment_atributes(self):
        query = f"""SELECT * FROM CALCULATED_ASSESSMENT WHERE ID_ASSESSMENT = {self.id_assessment}"""
        logger.info(f"[check_assessment_atributes] Query a base de datos para obtener posibles atributos de pantalla de valoración:\n{query}")
        rds_data = self.db_connection.execute(text(query))
        self.assessment_atributes = [row._asdict() for row in rds_data.fetchall()]
        self.assessment_atributes = self.assessment_atributes[0] if self.assessment_atributes else False
        
        
    @handler_wrapper('Se encontraron atributos de pantallla de valoración, adquiriendo atributos de FCLO', 'Atributos FCLO adquiridos', 'Error adquiriendo atributos FCLO', 'Error adquiriendo atributos fclo')
    def acquire_fclo_atributes(self):
        query = f"""SELECT * FROM FCLO_DISCOUNT WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY ITEM_DATE"""
        logger.info(f"[acquire_fclo_atributes] Query a base de datos para obtener atributos de FCLO:\n{query}")
        rds_data = self.db_connection.execute(text(query))
        self.fclo_atributes = [row._asdict() for row in rds_data.fetchall()]
        

    @handler_wrapper('Consumiendo atributos de fclo', 'Atributos de fclo consumidos con exito', 'Error consumiendo atributos de fclo', 'Error consumiendo fclo')
    def consume_fclo_atributes(self):
        fclo_dict = {'dates': []}
        if '-12-' in self.historic_dates[-1]:
            fclo_dict['dates'] = [self.historic_dates[-1]] + self.projection_dates
        
        else:
            self.projection_dates[0] = f'Diciembre {self.projection_dates[0]}'
            fclo_dict['dates'] = self.projection_dates
            

        logger.info(f'[fclo_dict] hasta aca {fclo_dict}')
        for bd_key, front_key in self.fclo_directory.items():
            fclo_dict[front_key] = [float(item[bd_key]) for item in self.fclo_atributes]
        logger.info(f'[fclo_dict] hasta aca {fclo_dict}')
        self.flow_discount.append(fclo_dict)

            
    
    @handler_wrapper('Consumiendo atributos base de la pantalla de valoración', 'Atributos base consumidos correctamente', 'Error consumiendo atributos base de la pantalla de valoración', 'Error consumiendo atributos de la pantalla de valoración1')
    def consume_assessment_atributes(self):
        for bd_key, front_key in self.assessment_directory.items():
            self.assessment[front_key] = always_ok(self.assessment_atributes[bd_key])
        self.assessment['efectAndBox'] = self.not_operational_accounts
        self.assessment['debtFinancialCost'] = self.debt_accounts


    @handler_wrapper('Organizando respuesta final', 'Respuesta final organizada exitosamente', 'Error organizando respuesta final', 'Error organizando respuesta final')
    def organize_partial_response(self):
        if self.assessment_atributes:
            self.assessment['flowDiscount'] = self.flow_discount
            self.partial_response = {'cash_flows': self.cash_flow_response, 'classifications': self.found_accounts, 'assessment': self.assessment}
        else:
            self.partial_response = {'cash_flows': self.cash_flow_response, 'classifications': self.found_accounts}


    @debugger_wrapper('Error construyendo respuesta final', 'Error construyendo respuesta')
    def response_maker(self, succesfull_run = False, error_str = str()):
        if succesfull_run:
            self.final_response['statusCode'] = 200
            self.final_response['body'] = json.dumps(self.partial_response)
            self.db_connection.commit() if __name__ != 'lambda_function' else None
                
        else:
            self.final_response['body'] = json.dumps(self.detailed_raise if self.detailed_raise else error_str)
            self.db_connection.rollback() if self.db_connection and __name__ != 'lambda_function' else None

        if self.db_connection:
            self.db_connection.close()
        return self.final_response


def always_ok(input_value):
    logger.debug(f'leyendo {input_value} de tipo {type(input_value)}')
    if type(input_value) is datetime.date:
        return input_value.strftime('%Y-%m-%d')
    
    elif type(input_value) is decimal.Decimal:
        return float(input_value)
        
    elif type(input_value) is int:
        return input_value
        
    elif type(input_value) is str:
        return input_value
    else:
        raise Exception('tipo no documentado')



def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)


if __name__ == "__main__":
    event = {"pathParameters": {"id_assessment": "51"}}
    lambda_handler(event, '')