import json
import logging
import sys
import os
from datetime import datetime
from sqlalchemy import text

from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db, connect_to_db_local_dev

logging.basicConfig()
logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', force=True, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


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

            event_dict = event['pathParameters']
            self.id_assessment = event_dict['id_assessment']

            self.dropdown_dict = {'fixedAssets': ['Activo Fijo', 'Activos intangibles'],    #Este es para crear el pool
                                  'period': ['Depreciación del Periodo', 'Amortización del Periodo'],           #Toca cambiar los vectores de los values por las clasificaciones que son
                                  'accumulated': ['Depreciación Acumulada', 'Amortización Acumulada']}
            
            self.retrieve_bd_dict = {'fixedAssets': 'ASSET_ACCOUNT',                #Este es para las propiedades basicas de cada tab, directorio de tabla FIXED_ASSETS
                                  'period': 'PERIOD_ACCOUNT',
                                  'acumulated': 'ACUMULATED_ACCOUNT'}
            
            self.vectors_directory = {'grossAssets': 'ASSET_VALUE',                 #Este es para las llaves de salida y la llave en BD de las proyecciones PROJECTED_FIXED_ASSETS
                                      'depAcumulated': 'ACUMULATED_VALUE', 
                                      'activeNeto': 'EXISTING_ASSET_VALUE', 
                                      'depPeriod': 'PERIOD_VALUE'}
            
            self.historic_vector_directory = {'ASSET_VALUE': 'fixedAssets',         #Este es para que según el vector a construir, saber la propiedad donde se ubica la clasificacion a buscar en los historicos
                                      'ACUMULATED_VALUE': 'acumulated', 
                                      'PERIOD_VALUE': 'period'}
            
            self.projections_found = list()
            self.accounts_found = set()
            self.id_items_groups = set()
            self.historic_dates = list()
            self.projection_dates = list()
            self.pool_result = list()
            self.tabs_result = list()

            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.create_conection_to_db()
            self.get_raw_classification()
            self.get_assessment_dates()
            #Construir el pool
            self.get_historic_summary_values()
            self.build_pool_master()

            logger.debug(f'[mira aca] el objeto de pool:\n{self.pool_result}')

            #Construir el retrieve
            self.get_assets_projections()
            if self.projections_found:
                self.organize_retrieve_tab_objects()
            
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


    @handler_wrapper('Adquiriendo raw de clasificaciones','Raw de clasificaciones adquirido con exito','Error adquiriendo raw de clasificaciones','Problemas adquiriendo clasificaciones')
    def get_raw_classification(self):
        query = """SELECT ID, CLASSIFICATION FROM RAW_CLASSIFICATION"""
        logger.info(f"[get_raw_classification] Query a base de datos para obtener el raw de clasificaciones:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        self.id_classification_dict = {str(row.ID) : row.CLASSIFICATION for row in rds_data.fetchall()}


    @handler_wrapper('Obteniendo las fechas del proceso de valoración', 'Fechas del proceso de valroación obtenidas con exito', 'Error obteniendo las fechas del proceso de valoración', 'Error obteniendo fechas del proceso de valoración')
    def get_assessment_dates(self):
        query = f"SELECT DATES, PROPERTY FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY DATES"
        logger.info(f"[get_assessment_dates] Query a base de datos para obtener las fechas utilizadas en el proces de valoración:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        directory = {'HISTORIC': self.historic_dates, 'PROJECTION': self.projection_dates}
        for date_item in rds_data.fetchall():
            directory.get(date_item.PROPERTY, []).append(date_item.DATES.strftime('%d-%m-%Y')) #Las self.projection_dates no las estoy usando para nada

        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')


    @handler_wrapper('Obteniendo valores historicos', 'Valores historicos obtenidos con éxito', 'Error obteniendo los valores historicos', 'Error adquiriendo data historica')
    def get_historic_summary_values(self):
        query = f"""SELECT C.ID as id_archive, C.INITIAL_DATE, B.CLASSIFICATION, D.ACCOUNT_NAME, A.ANNUALIZED FROM CLASSIFICATION_SUMMARY A, RAW_CLASSIFICATION B, ARCHIVE C, ASSESSMENT_CHECKED D
WHERE A.ID_RAW_CLASSIFICATION = B.ID AND C.ID = A.ID_ARCHIVE AND D.ID_ARCHIVE = C.ID AND D.ID_ASSESSMENT = A.ID_ASSESSMENT AND D.ID_RAW_CLASSIFICATION = A.ID_RAW_CLASSIFICATION
AND A.ID_ASSESSMENT = :id_assessment ORDER BY C.INITIAL_DATE, B.CLASSIFICATION""" #TODO, creo que así pongo un tercer orden por D.ACCOUNT_NUMBER, los pool van a llegar con las cuentas organizadas tambien

        logger.info(f'[get_historic_summary_values] Query para obtener los valores historicos:\n{query}')
        rds_data = self.db_connection.execute(text(query), {'id_assessment': self.id_assessment})
        self.historic_values = [item._asdict() for item in rds_data.fetchall()]
        for row in self.historic_values:
            row['ANNUALIZED'] = float(row['ANNUALIZED'])


    @handler_wrapper('Construyendo objeto pool', 'Objeto pool construído con éxito', 'Error construyendo objeto pool', 'Error construyendo pool de cuentas')
    def build_pool_master(self):
        archive_date_dict = {row['id_archive']: row['INITIAL_DATE'] for row in self.historic_values}
        for archive, date in archive_date_dict.items():
            archive_pool = {}
            for key, searching_classifitcations in self.dropdown_dict.items():
                archive_pool[key] = self.micro_key_pool_builder(archive, searching_classifitcations)

            archive_pool['date'] = date.strftime('%d-%m-%Y')
            self.pool_result.append(archive_pool)


    @debugger_wrapper('Error construyendo una llave de pool', 'Error construyendo pool de cuentas')
    def micro_key_pool_builder(self, archive, searching_classifitcations):
        key_response = []
        this_key_found_classifications = sorted(set(row['CLASSIFICATION'] for row in self.historic_values if row['CLASSIFICATION'].startswith(tuple(searching_classifitcations))))
        for classificacion in this_key_found_classifications:
            names = [row['ACCOUNT_NAME'] for row in self.historic_values if row['CLASSIFICATION'] == classificacion and row['id_archive'] == archive]
            total = next(row['ANNUALIZED'] for row in self.historic_values if row['CLASSIFICATION'] == classificacion and row['id_archive'] == archive)
            key_response.append({'groupName': classificacion, 'names': names, 'total': total})
        return key_response
    


    @handler_wrapper('Buscando Proyecciones de activos fijos', 'Proyecciones de activos fijos adquiridas con éxito', 'Error adquiriendo proyecciones de activos fijos','Error adquiriendo proyecciones de activos fijos')    
    def get_assets_projections(self):
        query = f"""SELECT A.ID_ITEMS_GROUP, A.PROJECTION_TYPE, A.ASSET_ACCOUNT, A.ACUMULATED_ACCOUNT, A.PERIOD_ACCOUNT, 
A.PROJECTED_YEARS, A.CALCULATION_COMMENT, B.PROJECTED_DATE, B.ASSET_VALUE, B.ACUMULATED_VALUE, B.EXISTING_ASSET_VALUE, B.PERIOD_VALUE
FROM FIXED_ASSETS A, PROJECTED_FIXED_ASSETS B WHERE  A.ID_ASSESSMENT = :id_assessment
AND A.ID_ASSESSMENT = B.ID_ASSESSMENT AND A.ID_ITEMS_GROUP = B.ID_ITEMS_GROUP ORDER BY B.PROJECTED_DATE"""

        logger.info(f'[get_assets_projections] Query para obtener las caracteristicas de las proyecciones de activos a depreciar: {query}')
        rds_data = self.db_connection.execute(text(query), {'id_assessment': self.id_assessment})
        self.projections_found = [item._asdict() for item in rds_data.fetchall()]
        for item in self.projections_found:
            possible_new_accounts = {item['ASSET_ACCOUNT'], item['ACUMULATED_ACCOUNT'], item['PERIOD_ACCOUNT']}
            self.accounts_found.update(possible_new_accounts) #esto no sé para qué lo iba a usar
            self.id_items_groups.add(item['ID_ITEMS_GROUP'])
            item['PROJECTED_DATE'] = item['PROJECTED_DATE'].strftime('%Y')
            item['ASSET_VALUE'] = float(item['ASSET_VALUE'])
            item['ACUMULATED_VALUE'] = float(item['ACUMULATED_VALUE'])
            item['EXISTING_ASSET_VALUE'] = float(item['EXISTING_ASSET_VALUE'])
            item['PERIOD_VALUE'] = float(item['PERIOD_VALUE'])


    @handler_wrapper('Tabs de proyecciones preconstruidos encontrado, organizando', 'tabs de proyecciones construídos con éxito', 'Error organizando tabs de proyecciones', 'Error organizando tabs de proyecciones')
    def organize_retrieve_tab_objects(self):
        self.id_items_groups = sorted(self.id_items_groups)
        for tab_id in self.id_items_groups:
            tab_object = {}
            properties_object = next(row for row in self.projections_found if row['ID_ITEMS_GROUP'] == tab_id)
            tab_object['dateHistorys'] = self.historic_dates
            tab_object['dateProjections'] = [row['PROJECTED_DATE'] for row in self.projections_found if row['ID_ITEMS_GROUP'] == tab_id]
            tab_object['dateProjections'][0] = f"Diciembre {tab_object['dateProjections'][0]}" if tab_object['dateProjections'][0] in self.historic_dates[-1] else tab_object['dateProjections'][0]
            tab_object['years'] = properties_object['PROJECTED_YEARS']
            tab_object['method'] = properties_object['PROJECTION_TYPE']

            for key, search_for in self.retrieve_bd_dict.items():
                tab_object[key] = self.id_classification_dict[properties_object[search_for]]
            
            for key, search_for in self.vectors_directory.items():
                tab_object[key] = self.create_tab_vectors(tab_id, search_for, tab_object)

            self.tabs_result.append(tab_object)

    @debugger_wrapper('Error creando tab', 'Error construyendo una de las pestañas de depreciacion')
    def create_tab_vectors(self, tab_id, bd_key_search_for, tab_object):
        if bd_key_search_for == 'EXISTING_ASSET_VALUE':
            historic_assect_vector = self.create_tab_vectors(tab_id, 'ASSET_VALUE', tab_object)['history']
            historic_acum_vector = self.create_tab_vectors(tab_id, 'ACUMULATED_VALUE', tab_object)['history']
            historic_vector = [i-j for i,j in zip(historic_assect_vector, historic_acum_vector)]
        else:
            historic_classificacion = tab_object[self.historic_vector_directory[bd_key_search_for]]
            historic_vector = {row['id_archive']: row['ANNUALIZED'] for row  in self.historic_values if row['CLASSIFICATION'] == historic_classificacion}
            historic_vector = list(historic_vector.values()) #Se hace un diccionario con los archives para que me traiga un valor único por archive

        proy_vector = [row[bd_key_search_for] for row in self.projections_found if row['ID_ITEMS_GROUP'] == tab_id]
        return {'history': historic_vector, 'projection': proy_vector}
        

    @handler_wrapper('Organizando body de respuesta', 'Body de respuesta organizado con éxito', 'Error organizando objeto de respuesta', 'Error organizando respuesta')
    def organize_partial_response(self):
        self.partial_response = {'data': self.pool_result, 'tabs': self.tabs_result}


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



def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    
if __name__ == "__main__":
    event = {"pathParameters": {"id_assessment": "2055"}}
    lambda_handler(event, '')