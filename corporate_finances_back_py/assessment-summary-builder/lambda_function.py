""":
capas:
capa-pandas-data-transfer

variables de entorno:
ASSESSMENT_CHECKED_TABLE : ASSESSMENT_CHECKED
CLASSIFICATION_SUMMARY_TABLE : CLASSIFICATION_SUMMARY
DB_SCHEMA : src_corporate_finance
RAW_CLASSIFICATION_TABLE : RAW_CLASSIFICATION
SECRET_DB_NAME : precia/rds8/sources/finanzas_corporativas
SECRET_DB_REGION : us-east-1

RAM: 1024 MB


"""

import json
import logging
import sys
import traceback
import time
import copy
from utils import *
import os 
import pandas as pd
from decorators import handler_wrapper, timing

#logging.basicConfig() #En lambdas borra este
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    lo = lambda_object(event)
    return lo.starter()
    
    
class lambda_object():

    @handler_wrapper('Obteniendo valores clave de event', 'Valores de event obtenidos correctamente',
               'Error al obtener valores event', 'Fallo en la obtencion de valores clave de event')
    def __init__(self, event) -> None:
        logger.warning(f'event que llega a la lambda: {str(event)}')

        event_body_json = event["body"]
        event_body_dict = json.loads(event_body_json)
        self.assessment_id = event_body_dict['assessment_id']
        self.archive_id = event_body_dict['archive_id']
        self.full_records = event_body_dict['full_records']
        self.parent_accounts_items = {'Ingresos operacionales': ['Ingresos operacionales 1', 'Ingresos operacionales 2', 'Ingresos operacionales 3', 'Ingresos operacionales 4', 'Ingresos operacionales 5'], 'Gastos operacionales': ['Gastos operacionales 1', 'Gastos operacionales 2', 'Gastos operacionales 3', 'Otros ingresos/egresos operativos']}
        self.special_classifications = ["Depreciación del periodo", "Depreciación acumulada", "Propiedad, planta y equipo", "Intangibles", "Amortización acumulada", "Amortización del periodo"]
        self.raw_classification_data = list()
        self.raw_classification_names = list()

        self.purged_items = list()
        self.calculated_summaries = list()

        self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'}}
        

    def starter(self):
        try:
            logger.info(f'[starter] Empezando starter de objeto lambda')
            self.create_conection_to_resources()
            self.get_classification_raw_info()
            self.classification_shooter()
            self.parent_accounts_shooter()
            self.special_classifications_shooter()
            self.annualize_information()
            self.merge_classifications_ids()
            self.create_uploable_dataframes()
            self.fix_dataframes()
            self.send_all_to_db()
            self.db_connection.close()
            return self.response_maker(succesfull = True)
            
        except Exception as e:
            if self.db_connection:
                self.db_connection.close()
            logger.error(f'[starter] Error en el procesamieno del comando de la linea: {get_current_error_line()}, motivo: {e}')
            return self.response_maker(succesfull = False, exception_str = str(e))


    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_resources(self):
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)


    @handler_wrapper('Trayendo todas las clasificaciones en raw_classifications','Clasificaciones obtenidas con exito', 'Error obteniendo clasificaciones raw', 'Error obteniendo clasificaciones raw')
    def get_classification_raw_info(self):
        query = f"SELECT * FROM RAW_CLASSIFICATION"
        logger.info(f'[get_classification_raw_info] Query para obtener las clasificaciones raw: {query}')
        query_result = self.db_connection.execute(query).mappings().all()
        self.raw_classification_data = [dict(item) for item in query_result]
        self.raw_classification_names = [item['CLASSIFICATION'] for item in self.raw_classification_data]


    @handler_wrapper('Inicializando disparador de clasificaciones', 'Disparador de clasificaciones terminado con exito', 'Error en el disparador de clasificaciones', 'Error calculando clasificacion')
    def classification_shooter(self):
        #[self.basic_seeker(master_classification) for master_classification in self.raw_classification_names] #me tengo que saltar la clasificacion 'No aplica'
        for master_classification in self.raw_classification_names:
            self.basic_seeker(master_classification)

            
    def basic_seeker(self, classification_to_find):
        logger.info(f'[basic_seeker] Calculando la clasificacion: "{classification_to_find}"')
        group_filter = lambda item: item['classification'] == classification_to_find
        found_items = list(filter(group_filter, self.full_records))
        if not found_items or classification_to_find == 'No aplica':
            logger.info(f'[basic_seeker] No se encontraron cuentas con clasificacion {classification_to_find}')
            return
        found_items = self.filter_items_to_min_level(found_items) 
        logger.warning(f"[basic_seeker] Los items con la clasificacion {classification_to_find} son: \n{found_items}")
        found_items = copy.deepcopy(found_items)
        self.individual_records_modifier(found_items)
        self.classification_summary_calculator(classification_to_find)
        

    @handler_wrapper('Inicializando disparador de clasificaciones padre', 'Disparador de cuentas padre terminado con exito', 'Error en el disparador cuentas padre', 'Error calculando cuentas padre de subs')
    def parent_accounts_shooter(self):
        [self.calculate_parents_accounts(parent_account, sub_classifications) for parent_account, sub_classifications in self.parent_accounts_items.items()]
    

    def calculate_parents_accounts(self, parent_account, sub_classifications):
        purged_items = list(filter(lambda item: item['classification'] in sub_classifications, self.purged_items))
        self.calculated_summaries.append({'classification': parent_account, **self.calculate_summary_groups(purged_items)})
    

    @handler_wrapper('Recalculando items de clasificaciones especiales', 'Clasificaciones especiales recalculadas con exito', 'Error al calcular cuentas especiales', 'Error calculando cuentas especiales')
    def special_classifications_shooter(self):
        logger.warning(f'purged_items antes de especiales: \n{self.purged_items}')
        [self.special_seeker(master_classification) for master_classification in self.special_classifications]
        logger.warning(f'purged_items despues de especiales: \n{self.purged_items}')


    def special_seeker(self, classification_to_find):
        logger.info(f'[special_seeker] Calculando la clasificacion: "{classification_to_find}"')
        f = lambda item : item['classification'] == classification_to_find
        found_items = list(filter(f, self.full_records))
        if found_items:
            sorted_unique_found_levels = sorted(set(item['nivel'] for item in found_items))
            if len(sorted_unique_found_levels) > 1:
                level_to_search = sorted_unique_found_levels[1]
                self.special_purged_accounts_eraser(classification_to_find)
                g = lambda item : item['nivel'] == level_to_search
                found_items = list(filter(g, found_items))
                found_items = copy.deepcopy(found_items)
                self.individual_records_modifier(found_items)
                return

            logger.info(f'[special_seeker] Todos los items de la clasificacion {classification_to_find} tienen solo un nivel')
            return 

        logger.info(f'[special_seeker] No se encontraron cuentas con clasificacion {classification_to_find}')


    def special_purged_accounts_eraser(self, classification_to_erase):
        self.purged_items = list(filter(lambda item:item['classification'] != classification_to_erase , self.purged_items))


    def individual_records_modifier(self, found_items):
        self.purged_items +=  list(map(self.purge_individual_item, found_items))
    

    def classification_summary_calculator(self, classification_to_find):
        purged_items = list(filter(lambda item: item['classification'] == classification_to_find, self.purged_items))
        self.calculated_summaries.append({'classification': classification_to_find, **self.calculate_summary_groups(purged_items)})


    def purge_individual_item(self, master_account):
        master_account['name'] = f"{master_account['name'].strip()} ({master_account['account']})" 
        master_account['hint'] = f"{master_account['account']} "
        filter_machine = self.create_sub_accounts_filter(master_account)
        self.sub_classifications_pool = list(filter(lambda item: all(f(item) for f in filter_machine), self.full_records))
        self.sub_classifications_pool.sort(key = lambda item: item['nivel'], reverse = False) #Con esto me aseguro que el set de subclasificaciones esté en orden de nivel y que no purgaré un nivel 10 antes de purgar un nivel 6
        sub_classifications = self.classifications_set_ordered(self.sub_classifications_pool)
        logger.warning(f"[purge_individual_item] sub clasificaciones encontradas para la cuenta {master_account['account']}: \n{sub_classifications}")
        
        for classification in sub_classifications:
            master_account['hint'] = master_account.get('hint', master_account['account']) #ver si esto lo puedo borrar
            logger.warning(f"[purge_individual_item] Cuenta {master_account['account']} antes de modificacion por clasificacion {classification}: \nValor: {master_account['balance']}\nHint: {master_account['hint']}")
            sub_classification_items = [item for item in self.sub_classifications_pool if item['classification'] == classification]
            sub_classification_items = self.filter_items_to_min_level(sub_classification_items)

            for sub_item in sub_classification_items:
                master_account = self.apply_sub_item_to_master(master_account, sub_item)

            logger.warning(f"[purge_individual_item] Cuenta {master_account['account']} post modificacion de clasificacion {classification}: \nvalor: {master_account['balance']}, hint: {master_account['hint']}")
        return master_account


    def create_sub_accounts_filter(self, master_account):
        filters = []
        filters.append(lambda item: item['account'].startswith(master_account['account']))
        filters.append(lambda item: item['classification'] != master_account['classification'])
        filters.append(lambda item: item['nivel'] > master_account['nivel'])
        #filters.append(lambda item: item['classification'] != 'No aplica')
        return filters


    def filter_items_to_min_level(self, items):
        nivels = sorted(set([item['nivel'] for item in items]))
        
        #usable_accounts = [ item['account'] for item in filter(lambda item: item['nivel'] == nivels[0], items)]
        usable_accounts = [ item['account'] for item in items if item['nivel'] == nivels[0] ]
        logger.info(f'[filter_items_to_min_level] usable_accounts original: \n{usable_accounts}')
        
        for nivel in nivels[1:]:
            this_level_items = [item for item in items if item['nivel'] == nivel]
            new_items = [item['account'] for item in this_level_items if not item['account'].startswith(tuple(usable_accounts))] 
            #Acá se están obteniendo las subcuentas que estén clasificadas igual que el subaccount del master pero que no sean subcuentas entre sí
            #suponga: master account 11 efectivo, subcuenta 1105 ppe, subcuenta 131015 ppe; estas dos subcuentas no son subcuentas entre sí, por eso ambas 
            #deben restarse del master account
            #si la subcuentas fueran 1110 ppe y 111020 ppe, debería tenerse en cuenta unicamente la 1110
            logger.info(f'[filter_items_to_min_level] se agregan las siguientes cuentas: \n{new_items}')
            usable_accounts.extend(new_items)
        return list(filter(lambda item: item['account'] in usable_accounts , items))


    def classifications_set_ordered(self, sub_classifications_pool):
        return list(dict.fromkeys(item['classification'] for item in sub_classifications_pool)) #Esto es para hacer un SET 
        #de las clasificaciones pero sin perder el orden de las mismas, porque ya están organizadas por nivel, requiero 
        #sacar las clasificaciones sin repetirse pero sin desorganizarse, que es lo que haría un set()


    def apply_sub_item_to_master(self, master_account, sub_item):
        master_account['balance'] = master_account['balance'] - sub_item['balance']
        master_account['hint'] = master_account['hint'] + f"-{sub_item['account']} " 
        
        self.sub_classifications_pool = list(filter(lambda item: not item['account'].startswith(sub_item['account']), self.sub_classifications_pool))
        #recordar que hay un sub_classification_items que son las sub cuentas con una clasificaciones, pero hay un sub_classifications_pool que contiene TODAS las subcuentas con 
        #multiples clasificaciones, las subcuentas con la misma clasificacion que se deban omitir ya se están omitiendo en filter_items_to_min_level, pero si hay sub clasificaciones 
        #con clasificaciones differentes en el pool, estas tambien se deben omitir, por eso es tan importante que la depuración se realice en niveles inferiores y luego a superiores
        return master_account
        
        
    #OJO esta definicion no se está usando
    def calculate_sub_grouping(self, item):
        value = 0
        accounts_used_log = ''

        #logger.warning(f"{item}")
        if (item['nature'] == 'Debito' and item['account'][0] in ['1', '2', '3']) or (item['nature'] == 'Credito' and item['account'][0] in ['4', '5', '6']):
            value = value + item['balance']
            accounts_used_log = accounts_used_log + f"+{item.get('hint', item.get('account'))} "
        if (item['nature'] == 'Credito' and item['account'][0] in ['1', '2', '3']) or (item['nature'] == 'Debito' and item['account'][0] in ['4', '5', '6']):
            value = value - item['balance']
            accounts_used_log = accounts_used_log + f"-{item.get('hint', item.get('account'))} "
        return {'value': value, 'hint':accounts_used_log}
    
    
    def calculate_summary_groups(self, found_purged_items):
        #ya que no se está teniendo en cuenta naturaleza y capitulos para el df2, 
        #lo ideal es que al sumar estas cuentas purgadas, sí se tenga en cuenta estas cosas
        #usar el algoritmo de calculate_sub_grouping
        summary_value = sum(item['balance'] for item in found_purged_items)
        summary_hint = ' + '.join(item['hint'] for item in found_purged_items)
        return {'value': summary_value, 'hint':summary_hint}


    def annualize_information(self):
        pass #acá va todo el master de anualización, sea manual o automatica


    @handler_wrapper('Emergiendo IDs de los nombres de clasificacion','Ids emergidos correctamente', 'Error emergiendo IDs de clasificacion', 'Error agrupando datos')
    def merge_classifications_ids(self):
        inverted_raw_classifications = {}

        for item in self.raw_classification_data:
            inverted_raw_classifications[item['CLASSIFICATION']] = item['ID']

        logger.warning(f'Este es mi elemento self.inverted_raw_classifications {inverted_raw_classifications}')
        for item in self.calculated_summaries:
            item['ID_RAW_CLASSIFICATION'] = inverted_raw_classifications[item['classification']]
        for item in self.purged_items:
            item['ID_RAW_CLASSIFICATION'] = inverted_raw_classifications[item['classification']]


    def create_uploable_dataframes(self):
        self.summaries_df = pd.DataFrame.from_records(self.calculated_summaries)
        self.purged_items_df = pd.DataFrame.from_records(self.purged_items)
        
        


    @handler_wrapper('Arreglando dataframes para subida a bd', 'Arreglo de dataframes terminado', 'Error arreglando dataframes', 'Error operando datos')
    def fix_dataframes(self):
        logger.warning(f"[fix_dataframes] summaries antes de fix: \n{self.summaries_df.to_string()}")
        self.summaries_df['ID_ASSESSMENT'] = self.assessment_id
        self.summaries_df['ID_ARCHIVE'] = self.archive_id
        self.summaries_df['ANNUALIZED'] = self.summaries_df['value']
        self.summaries_df.rename(columns={'value': 'CALCULATED_BALANCE', 'hint': 'HINT'},inplace=True)
        self.summaries_df.drop(['classification'], axis=1, inplace=True)
        #De este me falta el merge con id_raw_classifications
        
        logger.warning(f"[fix_dataframes] purgeds antes de fix: \n{self.purged_items_df.to_string()}")
        self.purged_items_df['ID_ASSESSMENT'] = self.assessment_id
        self.purged_items_df['ID_ARCHIVE'] = self.archive_id
        self.purged_items_df['ANNUALIZED'] = self.purged_items_df['balance']
        self.purged_items_df.rename(columns={'balance': 'CALCULATED_BALANCE', 'hint': 'HINT', 'nature': 'NATURE', 'account': 'ACCOUNT_NUMBER', 'name': 'ACCOUNT_NAME'},inplace=True)
        self.purged_items_df.drop(['initial_date', 'nivel', 'classification', 'chapter', 'status'], axis=1, inplace=True)  #este chapter es importante para despues, sería mejor no borrarlo
        #De este me falta el merge con id_raw_classifications
        
        logger.warning(f"[fix_dataframes] summaries despues de fix: \n{self.summaries_df.to_string()}")
        logger.warning(f"[fix_dataframes] purgeds despues de fix: \n{self.purged_items_df.to_string()}")
        #TODO: Tengo que sí o sí arreglar los hints y revisar los casos
        
        

    @handler_wrapper('Enviando informacion a base de datos', 'Información cargada correctamente', 'Error al cargar información a la base de datos', 'Hubieron problemas al cargar datos a base de datos')
    def send_all_to_db(self):
        self.summaries_df.to_sql(name="CLASSIFICATION_SUMMARY", con=self.db_connection, if_exists='append', index=False)
        
        self.purged_items_df.to_sql(name="ASSESSMENT_CHECKED", con=self.db_connection, if_exists='append', index=False)    

 
    def response_maker(self, succesfull = False, exception_str = str):
        if not succesfull:
            self.final_response['body'] = json.dumps(exception_str)
            return self.final_response
        else:
            self.final_response['body'] = json.dumps('ok')
            return self.final_response
            
    
def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)