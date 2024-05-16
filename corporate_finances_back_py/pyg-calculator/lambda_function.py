
import datetime
import json
import logging
import os
import sys
import sqlalchemy
import traceback
import copy
import pandas as pd

from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db, call_dynamic_engine
from vars import pyg_all_items, pyg_simple_calculations, pyg_partials, pyg_totals

#logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
#########################################
#######LAMBDA DEPRECIABLE################
#########################################

def lambda_handler(event, context):
    sc_obj = script_object(event)
    lambda_response = sc_obj.starter()
    logger.info(f'respuesta final de lambda: \n{lambda_response}')
    return lambda_response
    
    
class script_object():

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
    
            logger.warning(f'event de entrada: {str(event)}')
    
            self.id_assessment = event['pathParameters']['id_assessment']
    
            self.signs_inverter = '+-'.maketrans({'+':'-','-':'+'})
            
            self.historic_dates = list()
            self.projection_dates = list()
            self.total_asssessment_dates = 0
            
            self.pyg_table = dict()
            self.subs_results = list()
    
            self.pyg_values_vector = dict()
            self.pyg_hints_vector = dict()
            self.raw_pyg_id_dict = dict()
            
            self.capex_exists = False
            self.debt_exists = False
            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True


    def starter(self):
        try:
            logger.info(f'[starter] Empezando starter de objeto lambda')
            self.create_conection_to_db()
            self.get_assessment_dates()
            self.get_raw_pyg_data()
            self.initialize_zero_vectors()
            self.get_summary_results()
            self.pyg_calculator()
            self.calculate_partial_totals()
            
            #self.invert_period_dep_amo() #Colocar acá para invertir solo los historicos
            
            if self.projection_dates:
                self.get_projections_data()
                self.consume_projections_data()
                self.check_capex_existance()
                self.check_debt_existance()

            self.invert_period_dep_amo() #Al invertir acá estoy invirtiendo resultados de proyecciones también, creo que debo invertir solo los historicos
            
            if self.capex_exists: #Acá se debe sobreescribir amortizacion y depreciacion del periodo y la linea de capex
                self.consume_capex_data()
            else:
                del self.pyg_values_vector['Depreciación Capex']
                del self.pyg_hints_vector['Depreciación Capex']
            
            if self.debt_exists:
                self.consume_debt_data()
            
            self.calculate_pyg_totals()
            
            self.create_uploable_dataframes()
            self.check_previous_data()
            self.upload_dataframes_to_bd()
            call_dynamic_engine(self.id_assessment)
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


    @handler_wrapper('Obteniendo las fechas del proceso de valoración', 'Fechas del proceso de valroación obtenidas con exito', 'Error obteniendo las fechas del proceso de valoración', 'Error obteniendo fechas del proceso de valoración')
    def get_assessment_dates(self):
        query = f"SELECT DATES, PROPERTY FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY DATES"
        logger.info(f"[get_assessment_dates] Query a base de datos para obtener las fechas utilizadas en el proces de valoración:\n {query}")
        rds_data = self.db_connection.execute(query)
        directory = {'HISTORIC': self.historic_dates, 'PROJECTION': self.projection_dates}
        for date_item in rds_data.fetchall():
            directory.get(date_item.PROPERTY, []).append(date_item.DATES)
            self.total_asssessment_dates = self.total_asssessment_dates +1

        self.historic_dates = [date.strftime('%Y-%m-%d %H:%M:%S') for date in self.historic_dates]
        self.projection_dates = [date.strftime('%Y-%m-%d %H:%M:%S') for date in self.projection_dates]

        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')
        
        
    @handler_wrapper('Adquiriendo raw de pyg', 'Raw de pyg obtenido con exito', 'Error obteniendo datos Raw de pyg', 'Error adquiriendo identificadores de pyg')
    def get_raw_pyg_data(self):
        query = "SELECT * FROM RAW_PYG"
        logger.info(f"[get_raw_pyg_data] Query a base de datos para obtener el RAW PYG base:\n{query}")
        rds_data = self.db_connection.execute(query)
        self.raw_pyg = [item._asdict() for item in rds_data.fetchall()]
        self.raw_pyg_id_dict = {item['PYG_ITEM_NAME']:int(item['ID']) for item in self.raw_pyg}
        logger.info(f'[get_raw_pyg_data] Directorio adquirido de raw pyg:\n{self.raw_pyg_id_dict}')
    
    
    @handler_wrapper('Inicializando todos los vectores a zero', 'Vectores de flujo de caja inicializados', 'Error inicializando vectores de flujo de caja', 'Error iniciando sistema vectorial')
    def initialize_zero_vectors(self):
        for row in pyg_all_items:
            self.pyg_values_vector[row] = [0] * self.total_asssessment_dates
            self.pyg_hints_vector[row] = [''] * self.total_asssessment_dates
            

    @handler_wrapper('Obteniendo datos summary de clasificacion','summaries de clasificacion obtenidos','Error obteniendo summaries','Error al buscar informacion de sumaries')
    def get_summary_results(self):
        query = f"""SELECT B.CLASSIFICATION, A.HINT AS hint, A.ANNUALIZED AS value, B.IS_PARENT 
FROM CLASSIFICATION_SUMMARY A, RAW_CLASSIFICATION B, ARCHIVE C
WHERE A.ID_RAW_CLASSIFICATION = B.ID AND A.ID_ARCHIVE = C.ID
AND A.ID_ASSESSMENT = {self.id_assessment} ORDER BY C.INITIAL_DATE"""

        
        logger.info(f"[get_archive_info] Query a base de datos para obtener los summaries de clasificacion calculados:\n {query}")
        rds_data = self.db_connection.execute(query)
        
        self.summary_data = [dict(item) for item in rds_data.mappings().all()]
        for item in self.summary_data:
            item['value'] = float(item['value'])
        logger.warning(f'[get_summary_results] Summaries encontrados para el proceso de valoración:\n{self.summary_data}')


    @handler_wrapper('Empezando master calculador de pyg','Master calculador de pyg terminado','Error en el master de calculo para pyg','Error calculando')
    def pyg_calculator(self):
            
        for classification in pyg_simple_calculations:
            classification_value_vector, classification_hint_vector = self.filter_classification(classification)
            self.pyg_values_vector[classification] = classification_value_vector
            self.pyg_hints_vector[classification] = classification_hint_vector

        parents_accounts = sorted(set(item['CLASSIFICATION'] for item in self.summary_data if item['IS_PARENT']))
        logger.warning(f'[pyg_calculator] parents_accounts encontrados: \n{parents_accounts}')
        for parent in parents_accounts:
            self.filter_parents_of(parent)

        logger.warning(f'[pyg_calculator] Resultados pyg post calculadora\n**Vector de valores por clasificacion**\n{self.pyg_values_vector}\n\n**Vector de hints por clasificacion**\n') 
        

    @debugger_wrapper('Error filtrando clasificaciones','Error construyendo clasificaciones')
    def filter_classification(self, classification):
        result = [item for item in self.summary_data if item['CLASSIFICATION'] == classification]
        logger.warning(f'[filter_classification] Buscando clasificacion: {classification}, summary encontrado: {result}')
        if not result:
            return [0] * len(self.historic_dates), ['Clasificación no encontrada'] * len(self.historic_dates)
        
        value_vector = [item['value'] for item in result]
        hint_vector = [item['hint'] for item in result]
        return value_vector, hint_vector
        
        
    @debugger_wrapper('Error filtrando clasificaciones','Error construyendo clasificaciones')
    def filter_parents_of(self, parent):
        found_sons_classifications = sorted(set(son['CLASSIFICATION'] for son in self.summary_data if son['CLASSIFICATION'].startswith(parent) and (son['IS_PARENT'] == 0)))
        found_items_dict = {}
        logger.warning(f'[filter_parents_of] Agregando values y hints de la clasificacion padre {parent}, clasificaciones hijas encontradas:\n{found_sons_classifications}')
        for son_classification in found_sons_classifications: 
            classification_value_vector, classification_hint_vector = self.filter_classification(son_classification)
            self.pyg_values_vector[son_classification] = classification_value_vector
            self.pyg_hints_vector[son_classification] = classification_hint_vector


    @handler_wrapper('Calculando totales parciales', 'Totales parciales calculadas', 'Error calculando totales parciales', 'No se pudieron calcular "Otros ingresos y egresos"')
    def calculate_partial_totals(self):
        for total_partial, total_dict in pyg_partials.items():
            self.pyg_values_vector[total_partial] = self.calculate_total_vector(total_dict['dependencies'], total_dict['is_sum'])
            self.pyg_hints_vector[total_partial] = [' - '.join(total_dict['dependencies'])] * len(self.historic_dates)
            logger.info(f'[calculate_partial_totals] El vector de totales de {total_partial} obtenido es {self.pyg_values_vector[total_partial]}')


    @handler_wrapper('Realizando query a proyecciones del proceso de valoracion', 'Proyecciones adquiridas con exito', 'Error adquiriendo proyecciones de pyg', 'Error adquiriendo proyecciones')
    def get_projections_data(self):
        query = f"""SELECT B.PYG_ITEM_NAME as name, B.IS_SUB, A.VALUE AS value, C.PROJECTION_TYPE FROM PROJECTED_PYG A, RAW_PYG B, PYG_ITEM C
WHERE A.ID_RAW_PYG = B.ID AND C.ID_ASSESSMENT = A.ID_ASSESSMENT AND A.ID_RAW_PYG = C.ID_RAW_PYG AND A.ID_ASSESSMENT = {self.id_assessment} ORDER BY A.PROJECTED_DATE"""
                
        logger.info(f'[get_projections_data] Query para obtener proyecciones del proceso de valoracion:\n{query}')
        rds_data = self.db_connection.execute(query)
        self.projection_data =  [item._asdict() for item in rds_data.fetchall()]
        for item in self.projection_data:
            item['value'] = float(item['value'])
    
    

    @handler_wrapper('Proyecciones de pyg encontradas, consumiendo', 'Proyecciones de pyg consumidas con exito', 'Error consumiendo proyecciones de pyg', 'Error integrando proyecciones de pyg')
    def consume_projections_data(self):
        for pyg_item, pyg_values in self.pyg_values_vector.items():
            projection_vector = [item['value'] for item in self.projection_data if item['name'] == pyg_item]
            if not projection_vector:
                projection_vector = [0] * len(self.projection_dates)
            projection_hint = next((item['PROJECTION_TYPE'] for item in self.projection_data if item['name'] == pyg_item), 'proyeccion del item no encontrada')
            logger.info(f'[consume_projections_data] Vector de proyecciones encontradas para {pyg_item}:\n{projection_vector}\nCon hint:{projection_hint}') #\ndesde el objeto:.\n{self.projection_data}
            pyg_values.extend(projection_vector)
            self.pyg_hints_vector[pyg_item].extend([projection_hint] * len(self.projection_dates))

    
    @handler_wrapper('Chequeando si hay datos de capex', 'Chequeo de datos de capex terminado', 'Error chequeando si hay datos de capex', 'Error chequeando existencia de datos capex')
    def check_capex_existance(self):
        query = f"""SELECT * FROM CAPEX_SUMMARY WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY SUMMARY_DATE"""
                
        logger.info(f'[check_capex_existance] Query para obtener datos del summary de capex, si los hay:\n{query}')
        rds_data = self.db_connection.execute(query)
        self.capex_data =  [item._asdict() for item in rds_data.fetchall()]
        capex_dates_len = len(set(item['SUMMARY_DATE'] for item in self.capex_data))
        if capex_dates_len == self.total_asssessment_dates:
            self.capex_exists = True
            for row in self.capex_data:
                row['Depreciación del periodo'] = float(row['PERIOD_DEPRECIATION'])
                row['Amortización del periodo'] = float(row['PERIOD_AMORTIZATION'])
                #row['CAPEX'] = float(row['CAPEX'])
                row['Depreciación Capex'] = float(row['NEW_CAPEX'])

    @handler_wrapper('Chequeando si hay datos de capex', 'Chequeo de datos de capex terminado', 'Error chequeando si hay datos de capex', 'Error chequeando existencia de datos capex')
    def check_debt_existance(self):
        query = f"""SELECT INTEREST_VALUE FROM COUPLED_DEBT WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY SUMMARY_DATE"""
                
        logger.info(f'[check_debt_existance] Query para obtener datos del summary de deuda, si los hay:\n{query}')
        rds_data = self.db_connection.execute(query)
        self.debt_data =  [float(item.INTEREST_VALUE) for item in rds_data.fetchall()]
        if len(self.debt_data) == self.total_asssessment_dates:
            self.debt_exists = True


    @handler_wrapper('Invirtiendo depreciacion y amortización del periodo','Inversiones realizadas','Error invirtiendo depreciacion y amortización','Error invirtiendo resultados de depreciacion y amortización')
    def invert_period_dep_amo(self):
        for row in ['Depreciación del periodo', 'Amortización del periodo']:
            self.pyg_values_vector[row] = [-1* item for item in self.pyg_values_vector[row]]
            self.pyg_hints_vector[row] = [item.translate(self.signs_inverter) for item in self.pyg_hints_vector[row]]


    @handler_wrapper('Se encontraron datos de capex, consumiendo', 'Datos de capex consumidos con exito', 'Error consumiendo datos de capex', 'Error integrando datos de capex')
    def consume_capex_data(self):
        capex_vector_dict = {'Depreciación del periodo': [], 'Amortización del periodo': [], 'Depreciación Capex':[]} 
        for row in self.capex_data:
            capex_vector_dict['Depreciación del periodo'].append(row['Depreciación del periodo'])
            capex_vector_dict['Amortización del periodo'].append(row['Amortización del periodo'])
            capex_vector_dict['Depreciación Capex'].append(row['Depreciación Capex'])
            
        for key, vector in capex_vector_dict.items():
            self.pyg_values_vector[key] = vector
            self.pyg_hints_vector[key] = ['Modificado desde capex'] * self.total_asssessment_dates


    @handler_wrapper('Se encontraron resultados de deuda, consumiendo', 'Datos de deuda consumidos con exito', 'Error consumiendo datos de deuda', 'Error adjuntando datos de deuda')
    def consume_debt_data(self):
        self.pyg_values_vector['Intereses/gasto financiero'][-1 * len(self.projection_dates):] = self.debt_data[-1 * len(self.projection_dates):]
        self.pyg_hints_vector['Intereses/gasto financiero'][-1 * len(self.projection_dates):] = ['Modificado por existencia de deuda'] * len(self.projection_dates)
        

    @handler_wrapper('Calculando totales de pyg', 'Totales de pyg calculados con exito', 'Error calculando totales de pyg', 'Error calculando totales de pyg')
    def calculate_pyg_totals(self):
        for total_key, total_dict in pyg_totals.items():
            self.pyg_values_vector[total_key] = self.calculate_total_vector(total_dict['dependencies'], total_dict['is_sum'])
            self.pyg_hints_vector[total_key] = ['Total parcial Pyg'] * self.total_asssessment_dates
            logger.info(f'[calculate_pyg_totals] El vector de totales de {total_key} obtenido es {self.pyg_values_vector[total_key]}')
            
    
    @debugger_wrapper('Error calculando sub total de tabla', 'Error calculando totales de flujo de caja')
    def calculate_total_vector(self, dependencies, vector_signs):
        logger.info(f'[mira aca] dependencias que se estan inyectando:\n{dependencies}\ncon signos:{vector_signs}')
        dependencies_vectors = [self.pyg_values_vector.get(dependence, [0]*self.total_asssessment_dates) for dependence in dependencies]
        partial_vector = []
        logger.info(f'[mira aca] vectores de dependencias:\n{dependencies_vectors}')
        for year_values in zip(*dependencies_vectors):
            year_total = 0
            for index, value in enumerate(year_values):
                year_total = year_total + value * vector_signs[index]
            partial_vector.append(year_total)
        return partial_vector

        

    @handler_wrapper('Creando dataframes de carga a bd', 'Dataframes de carga a bd construídos con exito', 'Error creando dataframes de carga a bd', 'Error creando objetos de carga a bd')
    def create_uploable_dataframes(self):
        for item in ['Otros ingresos operativos', 'Otros egresos operativos', 'Otros ingresos no operativos', 'Otros egresos no operativos']:
            del self.pyg_values_vector[item]
            del self.pyg_hints_vector[item]
        logger.info(f'[create_uploable_dataframes] Estos son los datos que se van a pasar a dataframe:\n**Vectores de values:**\n{self.pyg_values_vector}\n\n**Vectores de hints:**\n{self.pyg_hints_vector}')
        df = pd.DataFrame.from_dict(self.pyg_values_vector).transpose()
        all_columns = ['pyg_raw'] + self.historic_dates + self.projection_dates
        df.reset_index(inplace = True)
        df.columns = all_columns
        self.to_db_df = df.melt(id_vars = 'pyg_raw', value_vars = self.historic_dates + self.projection_dates, var_name='DATES', value_name='VALUE')
        self.to_db_df['ID_ASSESSMENT'] = self.id_assessment
        self.to_db_df['ID_RAW_PYG'] = self.to_db_df['pyg_raw'].map(self.raw_pyg_id_dict)
        self.to_db_df['HINT'] = ''
        
        for key, hint_vector in self.pyg_hints_vector.items():
            self.to_db_df.loc[self.to_db_df['pyg_raw']==key, 'HINT'] = hint_vector 
        self.to_db_df.drop(['pyg_raw'], axis=1, inplace = True)
        
        #logger.info(f'[create_uploable_dataframes] DataFrame acomodado hasta este punto:\n{self.to_db_df.to_string()}')


    @handler_wrapper('Verificando que no existan datos anteriores', 'Borrado de posibles datos anteriores terminado', 'Error eliminando datos anteriores', 'Error sobreescribiendo datos anteriores')
    def check_previous_data(self):
        query = f"DELETE FROM PYG_RESULTS WHERE ID_ASSESSMENT = {self.id_assessment}"
        logger.info(f"[check_previous_data] Query a base de datos para eliminar posibles resultados anteriores de la tabla de pyg:\n{query}")
        rds_data = self.db_connection.execute(query)


    @handler_wrapper('Cargando dataframes a bd', 'Carga de datos de capex exitosa', 'Error cargando datos de capex a bd', 'Error cargando datos a bd')
    def upload_dataframes_to_bd(self):
        logger.info(f'[upload_dataframes_to_bd] Tabla que se está cargando a bd:\n{self.to_db_df.to_string()}')
        self.to_db_df.to_sql(name= 'PYG_RESULTS', con=self.db_connection, if_exists='append', index=False)
        

    @debugger_wrapper('Error construyendo respuesta final', 'Error construyendo respuesta')
    def response_maker(self, succesfull_run = False, error_str = str()):
        if self.db_connection:
            self.db_connection.close()
        if succesfull_run:
            self.final_response['statusCode'] = 200
            self.final_response['body'] = json.dumps('ok')
            return self.final_response
            
        self.final_response['body'] = json.dumps(self.detailed_raise if self.detailed_raise else error_str)
        return self.final_response


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)