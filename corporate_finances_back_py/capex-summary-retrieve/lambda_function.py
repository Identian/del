import json
import logging
import sys
import os

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

            event_dict = event['pathParameters']
            self.id_assessment = event_dict['id_assessment']
            
            self.organizer_directory = {'Ingresos operacionales':'OPERATIONAL_INCOME',
                                     'Activos existentes brutos':'EXISTING_ASSETS',
                                                         'CAPEX':'CAPEX',
                                            'Depreciación Capex':'NEW_CAPEX',
                                      'Depreciación del periodo': 'PERIOD_DEPRECIATION',
                                      'Amortización del periodo':'PERIOD_AMORTIZATION',
                                        'Depreciación acumulada':'ACUMULATED_DEPRECIATION',
                                        'Amortización acumulada':'ACUMULATED_AMORTIZATION',
                                       }
            
            #######################################
            self.summary_supplies = ['Ingresos operacionales', 'Propiedad, planta y equipo', 'Intangibles', 'Depreciación del periodo', 'Amortización del periodo', 'Depreciación acumulada', 'Amortización acumulada']
            
            self.summary_order = {'Ingresos operacionales': self.organize_operational_income, 
                                    'Activos existentes brutos': self.organize_assets,
                                    'CAPEX': self.organize_capex_line,
                                    'Depreciación Capex': self.organize_new_capex_line,
                                    'Depreciación del periodo': self.organize_dep_amrtz,
                                    'Amortización del periodo': self.organize_dep_amrtz,
                                    'Depreciación acumulada': self.organize_dep_amrtz,
                                    'Amortización acumulada': self.organize_dep_amrtz}
                                    
            self.period_classifications = ['Depreciación del periodo', 'Amortización del periodo']
            self.acumulated_classifications = ['Depreciación acumulada', 'Amortización acumulada']
            summary_classifications = self.period_classifications + self.acumulated_classifications
            self.dep_amrtz_data = {item: [] for item in summary_classifications}
            ########################################


            self.historic_dates = list()
            self.projection_dates = list()       
            self.summary_results = list()


            self.summary_data_found = list()
            self.historic_dates = list()
            self.operational_income_projections = list()
            self.historic_data = dict()
            self.proyection_dates = list()
            self.existant_assets_proyections = list()
            self.capex_summary_results = list()
            self.response_data = list()
            
            self.found_tabs = bool()
            self.found_new_capex = bool()
            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.create_conection_to_db()
            self.get_assessment_dates()
            self.get_summary_results()
            self.organize_response_data()
            if not self.summary_results:
                self.get_historic_summary_data()
                self.organize_historic_rows()
                self.get_operational_income_projections()
                self.get_dep_amrtz_projections()
                self.organize_dep_amrtz_projections()
                self.organize_assets_projections()
                self.get_capex_results()
                self.organize_summary()
            
            return self.response_maker(succesfull_run = True)
            
        except Exception as e:
            logger.error(f'[starter] Hubieron problemas en el comando de la linea: {get_current_error_line()}')
            return self.response_maker(succesfull_run = False, error_str = (str(e)))

    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_db(self):
        if __name__ != 'lambda_function':
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
        directory = {'HISTORIC': {'vector': self.historic_dates, 'out_fmt': '%d-%m-%Y'}, 'PROJECTION': {'vector': self.projection_dates, 'out_fmt': '%Y'}}
        for date_item in rds_data.fetchall():
            directory[date_item.PROPERTY]['vector'].append(date_item.DATES.strftime(directory[date_item.PROPERTY]['out_fmt'])) #Las self.projection_dates no las estoy usando para nada

        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')


    @handler_wrapper('Obteniendo los resultados de capex summary', 'Resultados de capex summary obtenidos con exito', 'Error obteniendo resultados de capex summary', 'Error obteniendo resultados de capex summary')
    def get_summary_results(self):
        query = """SELECT OPERATIONAL_INCOME, EXISTING_ASSETS, CAPEX, PERIOD_DEPRECIATION, PERIOD_AMORTIZATION, ACUMULATED_DEPRECIATION, 
ACUMULATED_AMORTIZATION, NEW_CAPEX FROM CAPEX_SUMMARY WHERE ID_ASSESSMENT = :id_assessment ORDER BY SUMMARY_DATE"""
        logger.info(f"[get_summary_results] Query a base de datos para obtener los resultados de summary:\n {query}")
        rds_data = self.db_connection.execute(text(query), {'id_assessment': self.id_assessment})
        self.summary_results = [row._asdict() for row in rds_data.fetchall()]
        for row in self.summary_results:
            row['OPERATIONAL_INCOME'] = float(row['OPERATIONAL_INCOME'])
            row['EXISTING_ASSETS'] = float(row['EXISTING_ASSETS'])
            row['CAPEX'] = float(row['CAPEX'])
            row['PERIOD_DEPRECIATION'] = float(row['PERIOD_DEPRECIATION'])
            row['PERIOD_AMORTIZATION'] = float(row['PERIOD_AMORTIZATION'])
            row['ACUMULATED_DEPRECIATION'] = float(row['ACUMULATED_DEPRECIATION'])
            row['ACUMULATED_AMORTIZATION'] = float(row['ACUMULATED_AMORTIZATION'])
            row['NEW_CAPEX'] = float(row['NEW_CAPEX'])

        logger.info(f'[summary_results] summary encontrado: {self.summary_results}')


    @handler_wrapper('Construyendo objeto data de respuesta', 'Data de respesta construída con éxito', 'Error construyendo objeto data de respuesta', 'Error organizando data de summary')
    def organize_response_data(self):
        data_response = []
        for key, search_for in self.organizer_directory.items():
            row_data = {'name': key}
            row_data['values'] = self.create_values_object(search_for)
            data_response.append(row_data)
        
        self.projection_dates[0] = f'Diciembre {self.projection_dates[0]}' if self.projection_dates[0] in self.historic_dates[-1] else self.projection_dates[0]
        self.partial_response = {'data': data_response, 'datesHistory': self.historic_dates, 'dateProjection': self.projection_dates}


    debugger_wrapper('Error creando micro objeto de summary', 'Error acomodando lineas de capex summary')
    def create_values_object(self, search_for):
        full_vector = [row[search_for] for row in self.summary_results]
        if len(full_vector) < (len(self.historic_dates) + len(self.projection_dates)):
            return {'history' : full_vector[:len(self.historic_dates)]}
        return {'history' : full_vector[:len(self.historic_dates)], 'projection': full_vector[len(self.historic_dates):]}


    @debugger_wrapper('Error construyendo respuesta final', 'Error construyendo respuesta')
    def response_maker(self, succesfull_run = False, error_str = str()):
        if succesfull_run:
            self.final_response['statusCode'] = 200
            self.final_response['body'] = json.dumps(self.partial_response)
        else:
            self.final_response['body'] = json.dumps(self.detailed_raise if self.detailed_raise else error_str)
        if self.db_connection:
                self.db_connection.close()
        return self.final_response



    @handler_wrapper('Buscando caracteristicas de capex existente', 'Caracteristicas de capex encontradas', 'Error adquiriendo caracteristicas capex','Error adquiriendo capex')
    def get_historic_summary_data(self):
        query = f"""SELECT D.INITIAL_DATE AS date, B.CLASSIFICATION, A.ANNUALIZED
FROM CLASSIFICATION_SUMMARY A, RAW_CLASSIFICATION B, ARCHIVE D
WHERE A.ID_RAW_CLASSIFICATION = B.ID AND A.ID_ARCHIVE = D.ID
AND ID_ASSESSMENT = {self.id_assessment} AND B.CLASSIFICATION IN ({str(self.summary_supplies).replace('[','').replace(']','')})
ORDER BY D.INITIAL_DATE"""

        logger.info(f'[get_capex_data] Query para obtener summaries de clasificacion:\n{query}')
        rds_data = self.db_connection.execute(text(query))
        self.summary_data_found = [item._asdict() for item in rds_data.fetchall()]

        for row in self.summary_data_found:
            row['ANNUALIZED'] = float(row['ANNUALIZED'])
            row['date'] = row['date'].strftime('%d-%m-%Y')
            if row['date'] not in self.historic_dates:
                self.historic_dates.append(row['date'])

        logger.info(f'[get_historic_summary_data] Fechas historicas encontradas: {self.historic_dates}')


    @handler_wrapper('Separando fechas historicas de proyecciones', 'Fechas separadas con exito', 'Error separando fechas historicas de proyecciones', 'Error acomodando datos')
    def organize_historic_rows(self):
        for classification in self.summary_order:
            self.historic_data[classification] = [item['ANNUALIZED'] for item in self.summary_data_found if item['CLASSIFICATION'] == classification]

        items_ppe = [item['ANNUALIZED'] for item in self.summary_data_found if item['CLASSIFICATION'] == 'Propiedad, planta y equipo']
        items_itb = [item['ANNUALIZED'] for item in self.summary_data_found if item['CLASSIFICATION'] == 'Intangibles']
        
        items_ppe = items_ppe if items_ppe else [0] * len(self.historic_dates)
        items_itb = items_itb if items_itb else [0] * len(self.historic_dates)
        
        self.historic_data['Activos existentes brutos'] = [i+j for i,j in zip(items_ppe, items_itb)]
        logger.info(f'[organize_historic_rows] Data historica lista para requerirse: \n{self.historic_data}')
    
    
    @handler_wrapper('Buscando proyecciones de ingresos operacionales', 'Proyecciones encontradas con exito', 'Error adquiriendo proyecciones de ingresos operacionales', 'Error adquiriendo proyecciones de ingresos operacionales')
    def get_operational_income_projections(self):
        query = f"""SELECT A.PROJECTED_DATE AS proy_date, A.VALUE FROM PROJECTED_PYG A, RAW_PYG B WHERE A.ID_RAW_PYG = B.ID
AND A.ID_ASSESSMENT = {self.id_assessment} AND B.PYG_ITEM_NAME = "Ingresos operacionales" ORDER BY A.PROJECTED_DATE"""

        logger.info(f'[get_operational_income_projections] Query para obtener proyecciones de ingresos operacionales:\n{query}')
        rds_data = self.db_connection.execute(text(query))
        operational_income_projections_data = [item._asdict() for item in rds_data.fetchall()]
        self.operational_income_projections = [float(item['VALUE']) for item in operational_income_projections_data]
        self.proyection_dates = [item['proy_date'].strftime('%d-%m-%Y') for item in operational_income_projections_data]
        
        logger.info(f'[get_operational_income_projections] proyecciones de ingresos operacionales encontradas: {self.operational_income_projections}')

    
    @handler_wrapper('Capex antiguo encontrado, organizando respuesta', 'Respuesta de lambda construída', 'Error construyendo respuesta de lambda', 'Error construyendo respuesta')
    def get_dep_amrtz_projections(self):
        query = f"""SELECT A.ID_ITEMS_GROUP, A.PROJECTED_DATE AS proy_date, A.ASSET_VALUE, A.ACUMULATED_VALUE, A.EXISTING_ASSET_VALUE, A.PERIOD_VALUE, C.ACCOUNT_NUMBER, D.CLASSIFICATION
FROM PROJECTED_FIXED_ASSETS A, FIXED_ASSETS B, ASSESSMENT_CHECKED C, RAW_CLASSIFICATION D, ASSESSMENT E
WHERE A.ID_ASSESSMENT = B.ID_ASSESSMENT AND A.ID_ASSESSMENT = C.ID_ASSESSMENT  AND A.ID_ASSESSMENT = E.ID
AND A.ID_ASSESSMENT = {self.id_assessment}
AND A.ID_ITEMS_GROUP = B.ID_ITEMS_GROUP AND E.ID_ARCHIVE = C.ID_ARCHIVE AND C.ID_RAW_CLASSIFICATION = D.ID
AND (B.ASSET_ACCOUNT = D.ID OR B.ACUMULATED_ACCOUNT = D.ID OR B.PERIOD_ACCOUNT = D.ID)
ORDER BY A.ID_ITEMS_GROUP, A.PROJECTED_DATE
"""

        logger.info(f'[get_dep_amrtz_projections] Query para obtener proyecciones de activos fijos:\n{query}')
        rds_data = self.db_connection.execute(text(query))
        self.assets_projections = [item._asdict() for item in rds_data.fetchall()]
        logger.info(f'[get_dep_amrtz_projections] proyecciones de activos fijos encontradas:\n{self.assets_projections}')
        for row in self.assets_projections:
            self.found_tabs = True
            row['ASSET_VALUE'] = float(row['ASSET_VALUE'])
            row['ACUMULATED_VALUE'] = float(row['ACUMULATED_VALUE'])
            row['EXISTING_ASSET_VALUE'] = float(row['EXISTING_ASSET_VALUE'])
            row['PERIOD_VALUE'] = float(row['PERIOD_VALUE'])

            row['proy_date'] = row['proy_date'].strftime('%d-%m-%Y')


    @handler_wrapper('Organizando proyecciones de assets', 'Proyecciones de assets organizadas con exito', 'Error organizando proyecciones de assets', 'Error sumarizando proyecciones de activos fijos')
    def organize_dep_amrtz_projections(self):

        for proy_date in self.proyection_dates:
            for classification in self.period_classifications:
                filtered_items = [item for item in self.assets_projections if (item['proy_date'] == proy_date and item['CLASSIFICATION'] == classification)]
                total_period = sum(item['PERIOD_VALUE'] for item in filtered_items)
                
                logger.info(f"""[organize_dep_amrtz_projections] items Filtrados para la clasificacion: {classification} en la fecha {proy_date}:\n{filtered_items}\n\
                total amortizacion/depreciacion del periodo:{total_period}""")
                
                self.dep_amrtz_data[classification].append(total_period)
            
            for classification in self.acumulated_classifications:
                filtered_items = [item for item in self.assets_projections if (item['proy_date'] == proy_date and item['CLASSIFICATION'] == classification)]
                total_acumluated = sum(item['ACUMULATED_VALUE'] for item in filtered_items)
                
                logger.info(f"""[organize_dep_amrtz_projections] items Filtrados para la clasificacion: {classification} en la fecha {proy_date}:\n{filtered_items}\n\
                total amortizacion/depreciacion acumulada:{total_acumluated}""")
                
                self.dep_amrtz_data[classification].append(total_acumluated)
                
        logger.warning(f'[organize_dep_amrtz_projections] Objeto final de depreciacion y amortización proyectada por pestañas de activos fijos:\n{self.dep_amrtz_data}')

    @handler_wrapper('Organizando proyecciones de activos fijos', 'Proyecciones de activos fijos organizadas', 'Error organizando proyeccionesde activos fijos', 'Error organizando calculos de activos fijos')
    def organize_assets_projections(self):
        for proy_date in self.proyection_dates:
            self.existant_assets_proyections.append(sum(set(item['ASSET_VALUE'] for item in self.assets_projections if item['proy_date'] == proy_date)))
        logger.info(f'[organize_assets_projections] Proyecciones encontradas para los assets de las tarjetas de depreciacion: {self.existant_assets_proyections}')

    @handler_wrapper('Adquiriendo los resultados de capex', 'Resultados de capex obtenidos con exito', 'Error adquiriendo los resultados de capex', 'Error re adquiriendo capex')
    def get_capex_results(self):
        query = f"SELECT CAPEX_SUMMARY, CAPEX_ACUMULATED FROM CAPEX_VALUES WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY CALCULATED_DATE"
        logger.info(f'[get_capex_results] Query para obtener los valores calculados de capex:\n{query}')
        rds_data = self.db_connection.execute(text(query))
        capex_results = [item._asdict() for item in rds_data.fetchall()]
        self.capex_summary_results = [float(item['CAPEX_SUMMARY']) for item in capex_results]
        self.new_capex_summary_results = [float(item['CAPEX_ACUMULATED']) for item in capex_results]
        logger.info(f'[get_capex_results] Valores calculados de capex encontrados:\n**Antiguo capex\n**{self.capex_summary_results}\n\n**Nuevo capex acumulado\n**{self.new_capex_summary_results}')
        if self.capex_summary_results:
            self.found_new_capex = True

    @handler_wrapper('Organizando summary para muestra en front', 'Summary organizado correctamente', 'Error organizando summary', 'Error organizando salida a front')
    def organize_summary(self):
        for row_summary, function in self.summary_order.items():
            created_row = function(row_summary)
            if not created_row:
                continue
            self.response_data.append(created_row)
        projection_dates_short = [date.split('-')[-1] for date in self.proyection_dates]

        projection_dates_short[0] = projection_dates_short[0] if '-12-' in self.historic_dates[-1] else f'Diciembre {projection_dates_short[0]}' #Se agrega esta linea para que llegue diciembre con formato de anualización a front

        self.partial_response = {'data': self.response_data, 'datesHistory': self.historic_dates, 'dateProjection': projection_dates_short}
        if not self.found_tabs and not self.found_new_capex:
            self.partial_response['dateProjection'] = []
            for item in self.partial_response['data']:
                item['values']['projection'] = []
        
    
    @debugger_wrapper('Error organizando row de ingreso operacional', 'Error organizando summary de ingreso operacional')
    def organize_operational_income(self, name):
        return {'name': name, 'values': {'history': self.historic_data['Ingresos operacionales'], 'projection': self.operational_income_projections}}
    
    
    @debugger_wrapper('Error organizando row de ingreso operacional', 'Error organizando summary de ingreso operacional')
    def organize_assets(self, name):
        return {'name': name, 'values': {'history': self.historic_data['Activos existentes brutos'], 'projection': self.existant_assets_proyections}}


    @debugger_wrapper('Error organizando row de ingreso operacional', 'Error organizando summary de ingreso operacional')
    def organize_capex_line(self, name):
        if not self.capex_summary_results:
            return None
        return {'name': name, 'values': {'history': self.capex_summary_results[:len(self.historic_dates)], 'projection': self.capex_summary_results[len(self.historic_dates):]}}
        
    @debugger_wrapper('Error organizando row de ingreso operacional', 'Error organizando summary de ingreso operacional')
    def organize_new_capex_line(self, name):
        if not self.new_capex_summary_results:
            return None
        return {'name': name, 'values': {'history': self.new_capex_summary_results[:len(self.historic_dates)], 'projection': self.new_capex_summary_results[len(self.historic_dates):]}}

    @debugger_wrapper('Error organizando row de ingreso operacional', 'Error organizando summary de ingreso operacional')
    def organize_dep_amrtz(self, name):
        historic_array = self.historic_data[name] if self.historic_data[name] else [0] * len(self.historic_dates)
        return {'name': name, 'values': {'history': historic_array, 'projection': self.dep_amrtz_data[name]}}


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    
if __name__ == "__main__":
    event = {"pathParameters": {"id_assessment": "2065"}}
    lambda_handler(event, '')