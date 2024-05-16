import json
import logging
import sys
import os

from datetime import datetime

from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db


#logging.basicConfig() #En lambdas borra este

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
            
            self.puc_chapters = {'1':'Activo', '2':'Pasivo', '3':'Patrimonio', '4':'Ingresos', '5':'Gastos', '6':'Costos de venta', '7':'Costos de producción o de operación', '8':'Cuentas de orden deudoras', '9':'Cuentas de orden acreedoras'}
            
            self.context_classifications = {'patrimony':['Aportes de capital social u otros','Cambios en el patrimonio'], 
                                                'wk':['Capital de trabajo'],
                                                'other_projections':['Otros movimientos que no son salida ni entrada de efectivo no operativos','Otros movimientos que no son salida ni entrada de efectivo operativos','Otros movimientos netos de activos operativos que afecta el FCLO','Otros movimientos netos de activos operativos que afecta el FCLA'],
                                                'debt' : ['Deuda con costo financiero']}
            
            self.purged_items = list()
            self.archives_ids = list()
            self.history_dates = list()
            self.key_items = list()
    

        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.create_conection_to_db()
            self.get_purged_items()
            self.create_items_keys()
            self.organize_pool_to_front()
            self.organize_partial_response()
            
            return self.response_maker(succesfull_run = True)
            
        except Exception as e:
            logger.error(f'[starter] Hubieron problemas en el comando de la linea: {get_current_error_line()}')
            return self.response_maker(succesfull_run = False, error_str = (str(e)))

    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_db(self):
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)


    @handler_wrapper('Obteniendo datos de puc purgados','Datos de puc obtenidos','Error obteniendo datos de puc','Error al buscar datos de puc purgados')
    def get_purged_items(self):
        searching_classifications = self.context_classifications[self.context]
        searching_classifications_str = str(searching_classifications).replace('[', '').replace(']','')
        query = f"""SELECT C.ID AS archive_id, C.INITIAL_DATE AS date, A.ACCOUNT_NUMBER AS account, B.CLASSIFICATION AS classification, A.ANNUALIZED AS value, A.ACCOUNT_NAME AS name
FROM ASSESSMENT_CHECKED A, RAW_CLASSIFICATION B, ARCHIVE C 
WHERE A.ID_RAW_CLASSIFICATION = B.ID AND A.ID_ARCHIVE = C.ID AND B.CLASSIFICATION IN ({searching_classifications_str})
AND ID_ASSESSMENT = {self.id_assessment} ORDER BY C.INITIAL_DATE"""
                
        logger.info(f"[get_purged_items] Query a base de datos para obtener los datos de puc calculados:\n {query}")
        rds_data = self.db_connection.execute(query)
        
        self.purged_items = [item._asdict() for item in rds_data.fetchall()]
        logger.info(f'[get_purged_items] Datos de cuentas traídas desde bd:\n{self.purged_items}')
        
        for item in self.purged_items:
            item['date'] = item['date'].strftime('%d-%m-%Y')
            if item['archive_id'] not in self.archives_ids:
                self.archives_ids.append(item['archive_id'])
                self.history_dates.append(item['date'])

            item['value'] = float(item['value'])
            #item['chapter'] = self.puc_chapters.get(item['account'][0], 'Capitulo no encontrado')

        logger.warning(f'[get_purged_items] datos de cuentyas post procesamiento inicial:\n{self.purged_items}')

    @handler_wrapper('Creando llaves para las listas desplegables', 'Listas desplegables creadas con exito', 'Error creando llaves de listas deplegables', 'Error creando listas desplegables')
    def create_items_keys(self):
        self.key_items = [{'name':item['name'], 'account':item['account']} for item in self.purged_items if item['archive_id'] == self.archives_ids[-1]]


    @handler_wrapper('Eliminando datos innecesarios de los objetos en el pool', 'Objetos pool limpiados con exito', 'Error limpiando objetos del pool', 'Error creando pool')
    def organize_pool_to_front(self):
        for item in self.purged_items:
            del item['name']
            del item['archive_id']


    @handler_wrapper('Organizando respuesta final', 'Respuesta final organizada con exito', 'Error organizando respeusta final', 'Error creando respesta final')
    def organize_partial_response(self):
        self.partial_response = {'dates': self.history_dates, 'items': self.key_items, 'pool': self.purged_items}


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
    