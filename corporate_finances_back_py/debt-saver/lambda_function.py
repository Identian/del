import json
import logging
import sys
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import text

from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db, call_dynamic_engine, connect_to_db_local_dev

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

            event_dict = json.loads(event['body'])
            self.id_assessment = event_dict['id_assessment']
            self.current_debt_items = event_dict['financialDebt']
            self.future_debt_items = event_dict['futureDebt']
            future_debt = event_dict.get('treasureDebt', False)
            if future_debt:
                future_debt = future_debt[0]
                future_debt['newAmount'] = 0
                logger.info('Se está capturando deuda de tesorería')
                self.future_debt_items.append(future_debt)
            
            self.historic_dates = list()
            self.debt_account_value_dict = dict()
            self.projection_dates = list()
            self.debt_directory = list()
            self.projections_directory = list()
            self.future_debt_directory = list()
            self.future_deb_projections_directory = list()
            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.create_conection_to_db()
            #self.db_connection.begin()
            self.get_assessment_dates()
            if self.current_debt_items:
                self.get_original_values()
                self.organize_debt_items()
                self.calculate_debt_projections()

            if self.future_debt_items:
                self.organize_future_items()
                self.calculate_future_debt_projections()
            
            self.delete_previous_bd_data()
            self.create_uploable_dataframes()
            self.upload_dataframes_to_bd()
            #self.db_connection.commit()
            call_dynamic_engine(self.id_assessment)

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
            directory.get(date_item.PROPERTY, []).append(date_item.DATES.strftime('%Y-%m-%d %H:%M:%S')) #Las self.projection_dates no las estoy usando para nada

        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')
    
    
    @handler_wrapper('Obteniendo valores originales de las deudas', 'Valores originales obtenidos con exito', 'Error adquiriendo valores originales de deudas', 'Error adquiriendo valores originales')
    def get_original_values(self):
        historic_debt_accounts = [item['accountNumber'] for item in self.current_debt_items]
        historic_debt_accounts_str = str(historic_debt_accounts).replace('[','').replace(']','')
        query = f"""SELECT A.ACCOUNT_NUMBER AS account, A.ANNUALIZED AS value FROM ASSESSMENT_CHECKED A, ASSESSMENT B WHERE A.ID_ASSESSMENT = B.ID 
        AND A.ID_ARCHIVE = B.ID_ARCHIVE AND A.ID_ASSESSMENT = {self.id_assessment} AND A.ACCOUNT_NUMBER IN ({historic_debt_accounts_str}) ORDER BY account"""
        
        logger.info(f"[get_original_values] Query a base de datos para obtener lo valores originales de deuda:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        
        self.debt_account_value_dict = {row.account:float(row.value) for row in rds_data.fetchall()}
        logger.info(f'[get_original_values] Valores originales de las cuentas de deudas:\n{self.debt_account_value_dict}')


    @handler_wrapper('Calculando proyecciones de deudas historicas', 'Proyecciones de deudas historicas calculados con exito', 'Error proyectando deuda historica', 'Error proyectando deuda historica')
    def organize_debt_items(self):
        for debt in self.current_debt_items:
            debt_caracteristics = {'ID_ASSESSMENT':self.id_assessment,
                                    'ACCOUNT_NUMBER': debt['accountNumber'], 
                                    'ORIGINAL_VALUE':self.debt_account_value_dict[debt['accountNumber']], 
                                    'ALIAS_NAME': debt['name'],
                                    'PROJECTION_TYPE': debt['method'],
                                    'START_YEAR': self.projection_dates[0].split('-')[0], 
                                    'ENDING_YEAR': debt['expiration'], 
                                    'DEBT_COMMENT': debt['explication'], 
                                    'RATE_COMMENT': debt['rates']['explication'], 
                                    'SPREAD_COMMENT': debt['spread']['explication']}
            self.debt_directory.append(debt_caracteristics)
     
     
    @handler_wrapper('Calculando proyecciones de deuda', 'Proyecciones de deuda calculadas con éxito', 'Error calculando proyecciones de deuda', 'Error calculando proyecciones de deuda')
    def calculate_debt_projections(self):
        for debt in self.current_debt_items:
            self.calculate_current_debt_projections(debt)
        logger.warning(f'[calculate_debt_projections] Directorio final de proyecciones de deuda historica:\n{self.projections_directory}')
 
 
    @debugger_wrapper('Error calculando proyeccion de deuda actual', 'Error calculando proyecciones de deuda actual')
    def calculate_current_debt_projections(self, debt):
        original_value = self.debt_account_value_dict[debt['accountNumber']]
        interest_vector = [float(i)+float(j) for i,j in zip(debt['rates']['values'], debt['spread']['values'])]
        
        #projection_years = int(debt['expiration']) - int(self.historic_dates[-1].split('-')[0]) #+ 1 # con esto corrijo la división de amortización
        projection_years = len(debt['years'])
        amortization_vector = [original_value / projection_years] * projection_years
        disbursement_vector = [0] * projection_years

        initial_balance_vector = [original_value - amortization_value * proy_year for proy_year, amortization_value in enumerate(amortization_vector)]
        final_balance_vector = initial_balance_vector[1:]

        final_balance_vector.append(0)
        interest_vector_balance = [i * j / 100 for i, j in zip(initial_balance_vector, interest_vector)]

        final_balance_variance_vector = [final_balance_vector[0]- initial_balance_vector[0]]
        for index, value in enumerate(final_balance_vector[1:], start = 1):
            final_balance_variance_vector.append(value - final_balance_vector[index-1])
        
        logger.info(f'[mira aca] \nvector de interés: {interest_vector_balance}\nanios de proyeccion: {projection_years}')
        
        projections_long_dates = [datetime.strptime(str(int(self.historic_dates[-1].split('-')[0]) + year), '%Y').strftime('%Y-%m-%d %H:%M:%S') for year in range(1, projection_years + 1)]

        for i in range(1, projection_years + 1):
            projection_item = {'ID_ASSESSMENT':self.id_assessment,
            'ACCOUNT_NUMBER':debt['accountNumber'], 
            'ALIAS_NAME':debt['name'], 
            'ITEM_DATE': projections_long_dates[i-1], 
            'INITIAL_BALANCE':initial_balance_vector[i-1], 
            'DISBURSEMENT':disbursement_vector[i-1], 
            'AMORTIZATION':amortization_vector[i-1], 
            'ENDING_BALANCE':final_balance_vector[i-1], 
            'INTEREST_VALUE':interest_vector_balance[i-1],
            'ENDING_BALANCE_VARIATION':final_balance_variance_vector[i-1],
            'RATE_ATRIBUTE':debt['rates']['values'][i-1],
            'SPREAD_ATRIBUTE':debt['spread']['values'][i-1]}
            self.projections_directory.append(projection_item)
            
        for date in self.historic_dates:
            projection_item = {'ID_ASSESSMENT':self.id_assessment,
            'ACCOUNT_NUMBER':debt['accountNumber'], 
            'ALIAS_NAME':debt['name'], 
            'ITEM_DATE': date, 
            'INITIAL_BALANCE':0, 
            'DISBURSEMENT':0, 
            'AMORTIZATION':0, 
            'ENDING_BALANCE':0, 
            'INTEREST_VALUE':0,
            'ENDING_BALANCE_VARIATION':0,
            'RATE_ATRIBUTE':0,
            'SPREAD_ATRIBUTE':0}
            self.projections_directory.append(projection_item)
            


    @handler_wrapper('Organizando items de deuda futura', 'Items de deuda futura organizados con exito', 'Error organizando items de deuda futura', 'Error organizando items de deuda futura')
    def organize_future_items(self):
        for debt in self.future_debt_items:
            debt_caracteristics = {'ID_ASSESSMENT':self.id_assessment,
                                    'ACCOUNT_NUMBER': 0, 
                                    'ORIGINAL_VALUE':debt['newAmount'], 
                                    'ALIAS_NAME': debt['name'],
                                    'PROJECTION_TYPE': debt['method'],
                                    'START_YEAR': debt['disburmentYear'], 
                                    'ENDING_YEAR': debt['finalYear'], 
                                    'DEBT_COMMENT': debt['explication'], 
                                    'RATE_COMMENT': debt['rates']['explication'], 
                                    'SPREAD_COMMENT': debt['spread']['explication']}
            self.future_debt_directory.append(debt_caracteristics)

    @handler_wrapper('Calculando proyecciones de deuda futura', 'Proyecciones de deuda futura calculadas con exito', 'Error calculando proyecciones de deuda futura', 'Error calculando proyecciones de deuda futura')
    def calculate_future_debt_projections(self):
        for debt in self.future_debt_items:
            self.calculate_single_future_debt_projection(debt)
        logger.warning(f'[calculate_future_debt_projections] Directorio construído de deuda futura:\n{self.projections_directory}')
    
    
    @debugger_wrapper('Error calculando proyeccion de deuda futura', 'Error calculando proyeccion de deuda futura')
    def calculate_single_future_debt_projection(self, debt):
        original_value = debt['newAmount']

        interest_vector = [i+j for i,j in zip(debt['rates']['values'], debt['spread']['values'])]
        
        projection_years = int(debt['finalYear']) - int(debt['disburmentYear']) + 1
        amortization_vector = [original_value / projection_years] * projection_years
        disbursement_vector = [0] * projection_years
        disbursement_vector[0] = original_value

        initial_balance_vector = [original_value - amortization_value * proy_year for proy_year, amortization_value in enumerate(amortization_vector)]
        final_balance_vector = initial_balance_vector[1:]   #esto está bien, mi saldo final va a ser el inicial del siguiente periodo

        final_balance_vector.append(0) #esto está bien, mi saldo final en el ultimo año debería ser cero
        logger.info(f' mira aca {initial_balance_vector} y {interest_vector}')
        interest_vector_balance = [i * safe_exit(j) / 100 for i, j in zip(initial_balance_vector, interest_vector)] #Al quitar la linea 238, el interés debería tener valores desde el primer año

        final_balance_variance_vector = [final_balance_vector[0]]
        for index, value in enumerate(final_balance_vector[1:], start = 1):
            final_balance_variance_vector.append(value - final_balance_vector[index-1])
        
        projections_long_dates = [datetime.strptime(str(int(debt['disburmentYear']) + year), '%Y').strftime('%Y-%m-%d %H:%M:%S') for year in range(projection_years)]

        for i in range(projection_years):
            projection_item = {'ID_ASSESSMENT':self.id_assessment,
            'ACCOUNT_NUMBER':0, 
            'ALIAS_NAME':debt['name'], 
            'ITEM_DATE': projections_long_dates[i],
            'INITIAL_BALANCE':initial_balance_vector[i], 
            'DISBURSEMENT':disbursement_vector[i], 
            'AMORTIZATION':amortization_vector[i], 
            'ENDING_BALANCE':final_balance_vector[i], 
            'INTEREST_VALUE':interest_vector_balance[i],
            'ENDING_BALANCE_VARIATION':final_balance_variance_vector[i],
            'RATE_ATRIBUTE':debt['rates']['values'][i],
            'SPREAD_ATRIBUTE':debt['spread']['values'][i]}
            self.future_deb_projections_directory.append(projection_item)
        

    @handler_wrapper('Eliminando posible data de deuda anterior', 'Posible data de deuda en bd eliminada correctamente', 'Error eliminando posible data de deuda en bd', 'Error sobreescribiendo data en bd')
    def delete_previous_bd_data(self):
        query = f"DELETE FROM DEBT WHERE ID_ASSESSMENT = {self.id_assessment}"
        logger.info(f'[delete_previous_bd_data] Query para eliminado de data en tabla DEBT:\n{query}')
        self.db_connection.execute(text(query))
        
        query = f"DELETE FROM PROJECTED_DEBT WHERE ID_ASSESSMENT = {self.id_assessment}"
        logger.info(f'[delete_previous_bd_data] Query para eliminado de data en tabla PROJECTED_DEBT:\n{query}')
        self.db_connection.execute(text(query))
        
    
    @handler_wrapper('Creando Dataframes para carga en bd', 'Dataframes creados con exito', 'Error creando dataframes para carga en bd', 'Error creando tablas para carga en bd')
    def create_uploable_dataframes(self):
        if self.current_debt_items:
            self.debt_df = pd.DataFrame.from_records(self.debt_directory)
            self.debt_projections_df = pd.DataFrame.from_records(self.projections_directory)
                    
        if self.future_debt_items:
            self.future_debt_df = pd.DataFrame.from_records(self.future_debt_directory) 
            self.future_debt_projections_df = pd.DataFrame.from_records(self.future_deb_projections_directory) 


    @handler_wrapper('Cargando data a bd', 'Data cargada a bd', 'Error en la carga de información a bd', 'Error cargando la información a bd')
    def upload_dataframes_to_bd(self):
        if self.current_debt_items:
            logger.info(f'[upload_dataframes_to_bd] Dataframe que se cargará a DEBT:\n{self.debt_df.to_string()}')
            logger.info(f'[upload_dataframes_to_bd] Dataframe que se cargará a PROJECTED_DEBT:\n{self.debt_projections_df.to_string()}')
            self.debt_df.to_sql(name='DEBT', con=self.db_connection, if_exists='append', index=False)
            self.debt_projections_df.to_sql(name='PROJECTED_DEBT', con=self.db_connection, if_exists='append', index=False)
        
        if self.future_debt_items:
            logger.info(f'[upload_dataframes_to_bd] Dataframe que se cargará a DEBT:\n{self.future_debt_df.to_string()}')
            logger.info(f'[upload_dataframes_to_bd] Dataframe que se cargará a PROJECTED_DEBT:\n{self.future_debt_projections_df.to_string()}')
            self.future_debt_df.to_sql(name='DEBT', con=self.db_connection, if_exists='append', index=False)
            self.future_debt_projections_df.to_sql(name='PROJECTED_DEBT', con=self.db_connection, if_exists='append', index=False)
            

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
    
def safe_exit(j):
    try:
        return float(j)
    except:
        return 0
    

if __name__ == "__main__":
    event = {"body": "{\"id_assessment\":2028,\"financialDebt\":[{\"accountName\":\"Pasivos financieras (21)\",\"accountNumber\":\"21\",\"expiration\":\"2022\",\"name\":\"Prueba1\",\"method\":\"Amortización lineal\",\"explication\":\"1\",\"years\":[\"Diciembre 2020\",2021,2022],\"rates\":{\"values\":[1,1,1],\"explication\":\"Hola\"},\"spread\":{\"values\":[1,1,1],\"explication\":\"Hola1\"}},{\"accountName\":\"Pasivos de operaciones corrientes (22)\",\"accountNumber\":\"22\",\"expiration\":\"2023\",\"name\":\"Prueba\",\"method\":\"Amortización lineal\",\"explication\":\"Purbea\",\"years\":[\"Diciembre 2020\",2021,2022,2023],\"rates\":{\"values\":[1,1,1,1],\"explication\":\"1\"},\"spread\":{\"values\":[1,1,1,1],\"explication\":\"1\"}},{\"accountName\":\"Cuentas por pagar (23)\",\"accountNumber\":\"23\",\"expiration\":\"2023\",\"name\":\"A\",\"method\":\"Amortización lineal\",\"explication\":\"1\",\"years\":[\"Diciembre 2020\",2021,2022,2023],\"rates\":{\"values\":[1,1,1,1],\"explication\":\"1\"},\"spread\":{\"values\":[1,1,1,-2],\"explication\":\"1\"}},{\"accountName\":\"Pasivos por impuestos corrientes (24)\",\"accountNumber\":\"24\",\"expiration\":\"2021\",\"name\":\"a\",\"method\":\"Amortización lineal\",\"explication\":\"1\",\"years\":[\"Diciembre 2020\",2021],\"rates\":{\"values\":[1,1],\"explication\":\"A\"},\"spread\":{\"values\":[1,-1],\"explication\":\"A\"}}],\"futureDebt\":[],\"treasureDebt\":[{\"disburmentYear\":2020,\"finalYear\":2023,\"name\":\"Deuda de Tesoreria\",\"method\":\"Amortización lineal\",\"explication\":\"\",\"years\":[\"2020\",\"2021\",\"2022\",\"2023\"],\"rates\":{\"values\":[0,0,0,0],\"explication\":\"\"},\"spread\":{\"values\":[0,0,0,0],\"explication\":\"\"}}]}"}
    lambda_handler(event, '')