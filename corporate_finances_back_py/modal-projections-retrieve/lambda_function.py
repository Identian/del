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
            self.id_assessment = event['queryStringParameters']['id_assessment']
            self.context = event['queryStringParameters']['context']

            self.context_classifications = {'patrimony':['Aportes de capital social u otros','Cambios en el patrimonio'], 
                                                'wk':['Capital de trabajo'],
                                                'other_projections':['Otros movimientos que no son salida ni entrada de efectivo no operativos','Otros movimientos que no son salida ni entrada de efectivo operativos','Otros movimientos netos de activos operativos que afecta el FCLO','Otros movimientos netos de activos operativos que afecta el FCLA'],
                                                'debt' : ['Deuda con costo financiero']}
            
            self.first_time_projecting_object = {'VS_ACCOUNT_NAME': 'Seleccione', 'PROJECTION_TYPE': 'Seleccione', 'COMMENT': ''}
            
            self.historic_dates = list()
            self.projection_dates = list()
            self.proy_years = int()
            self.projected_items = list()
            self.organized_projected_items = list()


        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.create_conection_to_db()
            self.get_assessment_dates()
            self.get_historic_data()
            self.get_projection_data()
            self.organize_atributes()
            self.organize_projected_items()
            self.organize_partial_response()
            return self.response_maker(succesfull = True)
             
        except Exception as e:
            logger.error(f'[starter] Hubieron problemas en el comando de la linea: {get_current_error_line()}')
            return self.response_maker(succesfull = False, error_str = (str(e)))

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

    
    @handler_wrapper('Obteniendo fechas de proyeccion del proceso de valoración', 'Fechas de proyeccion del proceso de valoración obtenidas con exito','Error adquiriendo fechas de proyeccion del proceso de valoración', 'Error adquiriendo fechas de proyeccion')
    def get_assessment_dates(self):
        query = f"SELECT DATES, PROPERTY FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY DATES"
        logger.info(f"[get_projection_dates] Query a base de datos para obtener las fechas utilizadas en el proceso de valoración:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        directory = {'HISTORIC': self.historic_dates, 'PROJECTION': self.projection_dates}
        for date_item in rds_data.fetchall():
            directory.get(date_item.PROPERTY, []).append(date_item.DATES)

        self.historic_dates = [date.strftime('%d-%m-%Y') for date in self.historic_dates]
        self.projection_dates = [date.strftime('%Y') for date in self.projection_dates] #'%Y-%m-%d %H:%M:%S'

        self.projection_dates[0] = f'Diciembre {self.projection_dates[0]}' if self.projection_dates[0] in self.historic_dates[-1] else self.projection_dates[0]
        self.proy_years = len(self.projection_dates) # if '-12-' in self.historic_dates[-1] else len(self.projection_dates) - 1 #quitar el comentario si no llegan los years que son a front
        
        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')


    @handler_wrapper('Obteniendo datos de puc purgados','Datos de puc obtenidos','Error obteniendo datos de puc','Error al buscar datos de puc purgados')
    def get_historic_data(self):
        query = f"""SELECT A.ACCOUNT_NUMBER, A.ANNUALIZED, C.CLASSIFICATION, A.ACCOUNT_NAME FROM ASSESSMENT_CHECKED A, ARCHIVE B, RAW_CLASSIFICATION C 
WHERE A.ID_ARCHIVE = B.ID AND A.ID_RAW_CLASSIFICATION = C.ID AND A.ID_ASSESSMENT = :id_assessment ORDER BY B.INITIAL_DATE, A.ACCOUNT_NUMBER;"""
                
        logger.info(f"[get_historic_data] Query a base de datos para obtener los datos proyectados:\n {query}")
        rds_data = self.db_connection.execute(text(query), {'id_assessment': self.id_assessment})
        self.historic_data = [item._asdict() for item in rds_data.fetchall()]
        for row in self.historic_data:
            row['ANNUALIZED'] = float(row['ANNUALIZED'])

        logger.info(f'[get_historic_data] Datos historicos de cuentas:\n{self.historic_data}')


    @handler_wrapper('Obteniendo datos de puc purgados','Datos de puc obtenidos','Error obteniendo datos de puc','Error al buscar datos de puc purgados')
    def get_projection_data(self):
        query = f"""SELECT A.ACCOUNT_NUMBER, A.VS_ACCOUNT_NAME, A.PROJECTION_TYPE, A.`COMMENT`, B.ATRIBUTE
FROM MODAL_WINDOWS A, MODAL_WINDOWS_PROJECTED B WHERE A.CONTEXT_WINDOW = B.CONTEXT_WINDOW 
AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER AND A.ID_ASSESSMENT = B.ID_ASSESSMENT 
AND A.ID_ASSESSMENT = :id_assessment AND A.CONTEXT_WINDOW = :context ORDER BY B.PROJECTED_DATE, A.ACCOUNT_NUMBER"""
                
        logger.info(f"[get_projection_data] Query a base de datos para obtener los datos proyectados:\n {query}")
        rds_data = self.db_connection.execute(text(query), {'id_assessment': self.id_assessment, 'context':self.context})
        self.projected_items = [item._asdict() for item in rds_data.fetchall()]
        logger.info(f'[get_projection_data] Datos de proyeccion:\n{self.projected_items}')

    
    @handler_wrapper('Organizando atributos de cuentas', 'Atributos de cuentas organizados con exito', 'Error organizando atributos de cuentas', 'Error organizando atributos')
    def organize_atributes(self):
        current_context_clasifications = self.context_classifications[self.context]
        self.context_accounts = sorted(set(row['ACCOUNT_NUMBER'] for row in self.historic_data if row['CLASSIFICATION'] in current_context_clasifications)) #con este tengo mi array de cuentas
        self.atributes_dict = {account: {'history': list(), 'projection': list()} for account in self.context_accounts}
        for account in self.context_accounts:
            self.atributes_dict[account]['history'] = [row['ANNUALIZED'] for row in self.historic_data if row['ACCOUNT_NUMBER'] == account]
            projection_vector = [row['ATRIBUTE'] for row in self.projected_items if row['ACCOUNT_NUMBER'] == account]
            if not projection_vector:
                projection_vector = [''] * self.proy_years
            self.atributes_dict[account]['projection'] = projection_vector


    @handler_wrapper('Eliminando datos innecesarios de los objetos en el pool', 'Objetos pool limpiados con exito', 'Error limpiando objetos del pool', 'Error creando pool')
    def organize_projected_items(self):
        for account in self.context_accounts:
            account_name = next(item for item in self.historic_data if item['ACCOUNT_NUMBER'] == account)['ACCOUNT_NAME']
            proy_info = next((item for item in self.projected_items if item['ACCOUNT_NUMBER'] == account), self.first_time_projecting_object)
            self.organized_projected_items.append({'name': account_name,
                                                    'account': account,
                                                    'accountProjector': proy_info['VS_ACCOUNT_NAME'], 
                                                    'atributes': self.atributes_dict[account], 
                                                    'explication': proy_info['COMMENT'], 
                                                    'method': proy_info['PROJECTION_TYPE']})




    @handler_wrapper('Organizando respuesta final', 'Respuesta final organizada con exito', 'Error organizando respeusta final', 'Error creando respesta final')
    def organize_partial_response(self):
        self.partial_response = {'datesProjections': self.projection_dates, 
                                'datesHistory': self.historic_dates, 
                                'year': self.proy_years, 
                                'projection_data': self.organized_projected_items, 
                                'context': self.context,
                                'id_assessment': self.id_assessment
        }


    def response_maker(self, succesfull=False, error_str=""):
        if self.db_connection:
            self.db_connection.close()
        if not succesfull:
            self.final_response["body"] = json.dumps(self.detailed_raise if self.detailed_raise else error_str)
        else:
            self.final_response["statusCode"] = 200
            self.final_response["body"] = json.dumps(self.partial_response)
        return self.final_response


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    
if __name__ == "__main__":
    event = {"queryStringParameters": {"id_assessment": "2064", "context": "wk"}}
    lambda_handler(event, '')


    