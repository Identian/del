import json
import logging
import sys
import os
import pandas as pd
import datetime
import os

from sqlalchemy import text

if __name__ in ['__main__', 'lambda_function']:
    from decorators import handler_wrapper, timing, debugger_wrapper
    from utils import get_secret, connect_to_db, call_dynamic_engine, connect_to_db_local_dev
else:
    from .decorators import handler_wrapper, timing, debugger_wrapper
    from .utils import get_secret, connect_to_db, call_dynamic_engine, connect_to_db_local_dev

logging.basicConfig()
logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(module)s] %(message)s', force=True, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

############################################
############ EN DESARROLLO PARA LA HU 4096
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
            self.partial_response = list()
    
            logger.warning(f'event de entrada: {str(event)}')

            event_dict = json.loads(event['body'])

            self.id_assessment = event_dict['id_assessment']
            self.capex_properties = event_dict['capex']
            
            self.historic_dates = list()
            self.projection_dates = list()
            self.capex_record = list()
            self.proy_dates = list()
            self.proy_records = list()
            
            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.create_conection_to_db()
            self.get_assessment_dates()
            self.organize_capex_result()
            self.organice_projection_result()
            self.create_uploable_dataframe()
            self.assets_safe_delete()
            self.upload_dataframes_to_bd()
            call_dynamic_engine(self.id_assessment, __name__)
            
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

    
    @handler_wrapper('Obteniendo las fechas del proceso de valoración', 'Fechas del proceso de valoración obtenidas con exito', 'Error obteniendo las fechas del proceso de valoración', 'Error obteniendo fechas del proceso de valoración')
    def get_assessment_dates(self):
        query = f"SELECT DATES, PROPERTY FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY DATES"
        logger.info(f"[get_assessment_dates] Query a base de datos para obtener las fechas utilizadas en el proces de valoración:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        directory = {'HISTORIC': self.historic_dates, 'PROJECTION': self.projection_dates}
        found_dates = [row._asdict() for row in rds_data.fetchall()]
        for date_item in found_dates:
            directory.get(date_item['PROPERTY'], []).append(date_item['DATES'].strftime('%Y-%m-%d %H:%M:%S'))
        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')
    

    @handler_wrapper('Organizando resultados capex de front', 'Resultados de capex organizados con exito', 'Error organizando resultados de capex', 'Error organizando datos a bd')
    def organize_capex_result(self):
        self.capex_record =[{'ID_ASSESSMENT': self.id_assessment, 
                             'CAPEX_NAME':capex_partial['name'],
                             'USED_ACCOUNT_NAME':capex_partial['accountProjector'], 
                             'METHOD': capex_partial['method'], 
                             'TIME_PERIOD':'ANUAL', 
                             'PERIODS': capex_partial['year'], 
                             'CALCULATION_COMMENT': ''} 
                             for capex_partial in self.capex_properties]
    
    @handler_wrapper('Organizando tabla de proyeccion', 'Datos de proyeccion organizados con exito', 'Error organizando datos de proyeccion', 'Error organizando proyeccion capex')
    def organice_projection_result(self):        
        
        for capex_property in self.capex_properties:
            for index, date in enumerate(self.historic_dates):
                proy_object = {
                    'ID_ASSESSMENT': self.id_assessment, 
                    'CAPEX_NAME':capex_property['name'],
                    'CALCULATED_DATE': date, 
                    'MANUAL_PERCENTAGE': float('nan'),
                    'CAPEX_SUMMARY': float('nan'),
                    'CAPEX_ACUMULATED': float('nan')
                }
            
                logger.info(f'[organize_capex_result] Objeto de capex historico a agregar:\n{proy_object}')
                self.proy_records.append(proy_object)

            for index, proy_date in enumerate(self.projection_dates):
                proy_object = {'ID_ASSESSMENT':self.id_assessment,
                                'CAPEX_NAME':capex_property['name'],
                                'CALCULATED_DATE': proy_date,
                                'MANUAL_PERCENTAGE': capex_property['manualValues'][index],
                                'CAPEX_SUMMARY': float('nan'),
                                'CAPEX_ACUMULATED': float('nan')
                }
                            
                logger.info(f'[organize_capex_result] Objeto de capex proyectado a agregar:\n{proy_object}')
                self.proy_records.append(proy_object)
        
    
    @handler_wrapper('Creando dataframes de carga', 'Dataframes de carga creados con exito', 'Error creando dataframes de carga', 'Error creando objeto para carga')
    def create_uploable_dataframe(self):
        self.capex_record_df = pd.DataFrame.from_records(self.capex_record)
        logger.info('[create_uploable_dataframe] df de capex a cargar:\n{self.capex_record_df.to_string()}')
        self.projection_records_df = pd.DataFrame.from_records(self.proy_records)
        logger.info('[create_uploable_dataframe] df de capex values a cargar:\n{self.projection_records_df.to_string()}')


    @handler_wrapper('Se requirió borrado en base de datos de información previa', 'Borrado exitoso', 'Error intentando eliminar información previa en bd', 'Error borrando información previa en bd')
    def assets_safe_delete(self):
        query = 'DELETE FROM CAPEX WHERE ID_ASSESSMENT = :id_assessment'
        logger.info(f'[assets_safe_delete] Query para ELIMINAR información previa en bd:\n{query}')
        self.db_connection.execute(text(query), {'id_assessment': self.id_assessment})

        query = 'DELETE FROM CAPEX_VALUES WHERE ID_ASSESSMENT = :id_assessment'
        logger.info(f'[assets_safe_delete] Query para ELIMINAR información previa en bd:\n{query}')
        self.db_connection.execute(text(query), {'id_assessment': self.id_assessment})
        

    @handler_wrapper('Cargando dataframes a bd', 'Carga de datos de capex exitosa', 'Error cargando datos de capex a bd', 'Error cargando datos a bd')
    def upload_dataframes_to_bd(self):
        self.capex_record_df.to_sql(name='CAPEX', con=self.db_connection, if_exists='append', index=False)
        self.projection_records_df.to_sql(name='CAPEX_VALUES', con=self.db_connection, if_exists='append', index=False)
    
    
    @debugger_wrapper('Error construyendo respuesta final', 'Error construyendo respuesta')
    def response_maker(self, succesfull_run = False, error_str = str()):
        if succesfull_run:
            self.final_response['statusCode'] = 200
            self.final_response['body'] = json.dumps('ok')
            self.db_connection.commit() if __name__ != 'lambda_function' else None
                
        else:
            self.final_response['body'] = json.dumps(self.detailed_raise if self.detailed_raise else error_str)
            self.db_connection.rollback() if self.db_connection and __name__ != 'lambda_function' else None

        if self.db_connection:
            self.db_connection.close()
        return self.final_response



def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    
if __name__ == '__main__':
    event = {'body': '{\"id_assessment\": 2071, \"capex\": [{\"name\": \"capex tab 1\", \"method\": \"Porcentaje de otra variable\", \"accountProjector\": \"Ingresos operacionales\", \"year\": \"5\", \"manualValues\": [10, 20, 30, 40, 50]}, {\"name\": \"capex tab 2\", \"method\": \"Manual\", \"accountProjector\": \"\", \"year\": \"5\", \"manualValues\": [100000, 200000, 300000, 400000, 500000]}]}'}
    lambda_handler(event, '')

    """
    EL OBJETO DE ENTRADA TIENE ESTA FORMA:
    {
    "id_assessment": 2071,
    "capex": [
        {
            "name": "capex tab 1",
            "method": "Porcentaje de otra variable",
            "accountProjector": "Ingresos operacionales",
            "year": "5",
            "manualValues": [10, 20, 30, 40, 50],
        },
		{
            "name": "capex tab 2",
            "method": "Manual",
            "accountProjector": "",
            "year": "5",
            "manualValues": [100000, 200000, 300000, 400000, 500000],
        }
    ]
}




    """