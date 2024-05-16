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

############################################
### CANDIDATO A DEPRECIACION POR HU 4095 ###
############################################

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
            self.partial_response = {}
    
            logger.warning(f'event de entrada: {str(event)}')

            event_dict = event['pathParameters']
            self.id_assessment = event_dict['id_assessment']
            
            
            self.drop_list_directory = {'gross_assestment_items': ["Propiedad, planta y equipo","Intangibles"],
                                'period_depreciation_items':["Depreciación del periodo", "Amortización del periodo"],
                                'acumulated_depreciation_items': ['Depreciación acumulada','Amortización acumulada']}
            
            self.drops_sets_list = dict()
            
            self.all_archives_ids = list()
            self.items_pool = list()
            self.historic_dates = list()
            self.short_dates = list()
            self.archives_ids = list()
            self.cleaned_items_pool = list()
            
            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            
            self.create_conection_to_db()
            self.get_user_classificated_data()
            self.filter_drop_down_pools()
            self.clearing_objects_pool()
            self.organize_final_response()
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


    @handler_wrapper('Buscando clasificacion master manual', 'Master asignado correctamente', 'Error adquiriendo master de clasificacion','Error adquiriendo clasificacion de analista')    
    def get_user_classificated_data(self):
        classifications_to_search_for = []
        list(map(lambda values: classifications_to_search_for.extend(values), self.drop_list_directory.values()))
        
        query = f"""SELECT C.INITIAL_DATE AS date, C.ID, B.CLASSIFICATION, A.ACCOUNT_NUMBER, A.ACCOUNT_NAME, A.ANNUALIZED, A.HINT
FROM ASSESSMENT_CHECKED A, RAW_CLASSIFICATION B, ARCHIVE C
WHERE A.ID_ARCHIVE = C.ID AND A.ID_RAW_CLASSIFICATION = B.ID
AND A.ID_ASSESSMENT = {self.id_assessment} AND B.CLASSIFICATION IN {tuple(classifications_to_search_for)}
ORDER BY C.INITIAL_DATE"""

        logger.info(f'[get_user_classificated_data] Query para obtener los pasos completados del proceso de valoracion:\n{query}')
        rds_data = self.db_connection.execute(query)
        self.items_pool = [item._asdict() for item in rds_data.fetchall()]
        for item in self.items_pool:
            item['ANNUALIZED'] = float(item['ANNUALIZED'])
            if item['ID'] not in self.archives_ids:
                self.archives_ids.append(item['ID'])
                self.historic_dates.append(item['date'])
                self.short_dates.append(item['date'].strftime('%d-%m-%Y'))
                
        logger.info(f'[get_user_classificated_data] Fechas historicas encontradas: {self.historic_dates}')
        logger.info(f'[get_user_classificated_data] archives historicos encontrados: {self.archives_ids}')


    @handler_wrapper('Creando pool filtrado de listas desplegazbles','filtrado completado','Error filtrando ','Error obteniendo Pucs')
    def filter_drop_down_pools(self):
        for drop_name, classifications in self.drop_list_directory.items():
            self.drops_sets_list[drop_name] = [{'name': item['ACCOUNT_NAME'], 'account':item['ACCOUNT_NUMBER']} for item in self.items_pool if (item['CLASSIFICATION'] in classifications) and (item['ID'] == self.archives_ids[-1])]
            logger.info(f'[filter_drop_down_pools] Items encontrados para el drop down {drop_name}:\n{self.drops_sets_list[drop_name]}')

    @handler_wrapper('Organizando pool de items', 'Pool de items organizado con exito', 'Error organizando pool de items', 'Error organizando pool de items')
    def clearing_objects_pool(self):
        for item in self.items_pool:
            this_item_date = item['date'].strftime('%d-%m-%Y')
            self.cleaned_items_pool.append({'account': item['ACCOUNT_NUMBER'], 'date': this_item_date, 'value': item['ANNUALIZED'], 'classification': item['CLASSIFICATION']})

    @handler_wrapper('Organizando respuesta final', 'Respuesta de lambda organizada con exito', 'Error organizando respuesta de lambda', 'Error organizando respuesta')
    def organize_final_response(self):
        self.partial_response = {'dates': self.short_dates, **self.drops_sets_list, 'pool': self.cleaned_items_pool}


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
    