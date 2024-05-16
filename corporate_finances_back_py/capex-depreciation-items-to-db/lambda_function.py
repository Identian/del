import json
import logging
import sys
import os
import datetime
import pandas as pd
from sqlalchemy import text

from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db, call_dynamic_engine, connect_to_db_local_dev

 
logging.basicConfig()
logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', force=True, datefmt='%Y-%m-%d %H:%M:%S')
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
            self.tabs = event_dict['tabs']
            
            self.assets_records = list()
            self.projection_records = list()
            self.historic_dates = list()
            self.projection_dates = list()
            self.classification_id_dict = dict()
            
            
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
            self.assets_safe_delete()
            self.organize_assets_tabs()
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


    @handler_wrapper('Adquiriendo raw de clasificaciones','Raw de clasificaciones adquirido con exito','Error adquiriendo raw de clasificaciones','Problemas adquiriendo clasificaciones')
    def get_raw_classification(self):
        query = """SELECT ID, CLASSIFICATION FROM RAW_CLASSIFICATION"""
        logger.info(f"[get_raw_classification] Query a base de datos para obtener el raw de clasificaciones:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        self.classification_id_dict = {row.CLASSIFICATION: row.ID for row in rds_data.fetchall()}


    @handler_wrapper('Obteniendo las fechas del proceso de valoración', 'Fechas del proceso de valroación obtenidas con exito', 'Error obteniendo las fechas del proceso de valoración', 'Error obteniendo fechas del proceso de valoración')
    def get_assessment_dates(self):
        query = f"SELECT DATES, PROPERTY FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY DATES"
        logger.info(f"[get_assessment_dates] Query a base de datos para obtener las fechas utilizadas en el proces de valoración:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        directory = {'HISTORIC': self.historic_dates, 'PROJECTION': self.projection_dates}
        for date_item in rds_data.fetchall():
            directory.get(date_item.PROPERTY, []).append(date_item.DATES.strftime('%Y-%m-%d %H:%M:%S')) #Las self.projection_dates no las estoy usando para nada

        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')


    @handler_wrapper('Borrando información previa en base de datos', 'Borrado exitoso', 'Error intentando eliminar información previa en bd', 'Error borrando información previa en bd')
    def assets_safe_delete(self):
        query = f'DELETE FROM FIXED_ASSETS WHERE ID_ASSESSMENT = :id_assessment'
        logger.info(f'[assets_safe_delete] Query para ELIMINAR información previa en bd:\n{query}')
        self.db_connection.execute(text(query), {'id_assessment': self.id_assessment})

        query = f'DELETE FROM PROJECTED_FIXED_ASSETS WHERE ID_ASSESSMENT = id_assessment'
        logger.info(f'[assets_safe_delete] Query para ELIMINAR información previa en bd:\n{query}')
        self.db_connection.execute(text(query), {'id_assessment': self.id_assessment})
        

    @handler_wrapper('Construyendo tablas a bd', 'Tablas a bd construídas con exito', 'Error construyendo tablas a bd','Error construyendo tablas a bd')
    def organize_assets_tabs(self):
        for id_items_group, tab_properties in enumerate(self.tabs):
            tab_record = {'ID_ASSESSMENT': self.id_assessment,
                            'ID_ITEMS_GROUP': id_items_group, 
                            'PROJECTION_TYPE': tab_properties['method'],
                            'ASSET_ACCOUNT': self.classification_id_dict[tab_properties['fixedAssets']],
                            'PERIOD_ACCOUNT': self.classification_id_dict[tab_properties['period']],
                            'ACUMULATED_ACCOUNT': self.classification_id_dict[tab_properties['acumulated']],
                            'PROJECTED_YEARS': tab_properties['years'],
            }
            self.assets_records.append(tab_record)
            self.create_proyected_records(id_items_group, tab_properties)


    @debugger_wrapper('Error construyendo records de proyeccion de activos fijos', 'Error construyendo records de guardado')
    def create_proyected_records(self, id_items_group, tab_properties):
        tab_dates = [self.projection_dates[0]]
        first_projection_year = datetime.datetime.strptime(self.projection_dates[0], '%Y-%m-%d %H:%M:%S').replace(day = 1, month = 1)
        tab_dates = tab_dates + [first_projection_year.replace(year = first_projection_year.year + year).strftime('%Y-%m-%d %H:%M:%S') for year in range(1, len(tab_properties['grossAssets']['projection']) + 1)]
        
        for index, proy_dates in enumerate(tab_dates):
            try:
                proy_record = {
                'ID_ASSESSMENT': self.id_assessment,
                'ID_ITEMS_GROUP': id_items_group,
                'PROJECTED_DATE': proy_dates,
                'ASSET_VALUE': tab_properties['grossAssets']['projection'][index],
                'ACUMULATED_VALUE': tab_properties['depAcumulated']['projection'][index],
                'EXISTING_ASSET_VALUE': tab_properties['activeNeto']['projection'][index],
                'PERIOD_VALUE': tab_properties['depPeriod']['projection'][index],
                }
                self.projection_records.append(proy_record)
            except Exception as e:
                logger.error(f'[mira aca] {get_current_error_line()} el objeto es {tab_properties}')
                break
            


    @handler_wrapper('Cargando data a bd', 'Data carga a bd', 'Error en la carga de información a bd', 'Error cargando la información a bd')
    def upload_dataframes_to_bd(self):
        self.assets_records_df = pd.DataFrame.from_records(self.assets_records)
        self.projection_records_df = pd.DataFrame.from_records(self.projection_records)
        logger.info(f'a guardar en fixed:\n{self.assets_records_df.to_string()}')
        logger.info(f'a guardar en projected:\n{self.projection_records_df.to_string()}')
        self.assets_records_df.to_sql(name='FIXED_ASSETS', con=self.db_connection, if_exists='append', index=False)
        self.projection_records_df.to_sql(name='PROJECTED_FIXED_ASSETS', con=self.db_connection, if_exists='append', index=False)


    def response_maker(self, succesfull_run = False, error_str = str):
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
    


if __name__ == "__main__":
    event = {'body': '{"id_assessment": 2055, "data": [{"date": "30/03/2024", "fixedAssets": [{"groupName": "Activo Fijo 1", "names": ["Activo Cuenta 1", "Activo Cuenta 2", "Activo Cuenta 3"], "total": 30}], "period": [{"groupName": "Depreciacion del Periodo", "names": ["Depreciacion Periodo Cuenta 1", "Depreciacion Periodo Cuenta 2", "Depreciacion Periodo Cuenta 3"], "total": 30}], "accumulated": [{"groupName": "Depreciacion Acumulada", "names": ["Depreciacion Cuenta 1", "Depreciacion Cuenta 2", "Depreciacion Cuenta 3"], "total": 30}]}, {"date": "30/03/2023", "fixedAssets": [{"groupName": "Activo Fijo 1", "names": ["Daniel", "Hola", "Estas"], "total": 30}], "period": [{"groupName": "Depreciacion del Periodo", "names": ["Jeisson", "Hola", "Esta"], "total": 30}], "accumulated": [{"groupName": "Depreciacion Acumulada", "names": ["Fernando", "Hola", "Esta"], "total": 30}]}], "tabs": [{"fixedAssets": "Propiedad, planta y equipo", "period": "Ingresos operacionales 1", "acumulated": "Gastos operacionales 1", "grossAssets": {"history": [31951.43], "projection": [31951.43, 31951.43, 31951.43, 31951.43, 31951.43]}, "depAcumulated": {"history": [10118.08], "projection": [14484.75, 18851.42, 23218.09, 27584.76, 31951.43]}, "activeNeto": {"history": [21833.35], "projection": [17466.68, 13100.01, 8733.34, 4366.67, 0]}, "depPeriod": {"history": [6286.42], "projection": [4366.67, 4366.67, 4366.67, 4366.67, 4366.67]}, "dateHistorys": ["31-12-2022"], "dateProjections": ["2023", "2024", "2025", "2026", "2027"], "years": 5, "method": "D&A en línea recta"}, {"fixedAssets": "Intangibles", "period": "Ingresos operacionales 2", "acumulated": "Gastos operacionales 2", "grossAssets": {"history": [31951.43], "projection": [31951.43, 31951.43, 31951.43, 31951.43, 31951.43]}, "depAcumulated": {"history": [10118.08], "projection": [14484.75, 18851.42, 23218.09, 27584.76, 31951.43]}, "activeNeto": {"history": [21833.35], "projection": [17466.68, 13100.01, 8733.34, 4366.67, 0]}, "depPeriod": {"history": [6286.42], "projection": [4366.67, 4366.67, 4366.67, 4366.67, 4366.67]}, "dateHistorys": ["31-12-2022"], "dateProjections": ["2023", "2024", "2025", "2026", "2027"], "years": 5, "method": "D&A en línea recta"}]}'}
    lambda_handler(event, '')