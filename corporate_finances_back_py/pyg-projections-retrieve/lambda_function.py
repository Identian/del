import json
import logging
import sys
import os
import datetime
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
            self.partial_response = []
    
            logger.warning(f'event de entrada: {str(event)}')

            self.accounts_to_skip = ("EBITDA", "Utilidad bruta", "EBIT", "Utilidad antes de impuestos", "Utilidad neta", "Ingresos operacionales", "Gastos operacionales")
            self.pyg_totals = {'Otros ingresos y egresos operativos': {'dependencies': ['Otros ingresos operativos','Otros egresos operativos'],'is_sum':[1,-1]},
                                    'Otros ingresos y egresos no operativos': {'dependencies': ['Otros ingresos no operativos','Otros egresos no operativos'], 'is_sum': [1,-1]}}
            
            self.projections_order = {'Ingresos operacionales': True,
                                    'Costos (Sin depreciación)': False, 
                                    'Gastos operacionales': True,
                                    'Otros ingresos y egresos operativos': False,
                                    #'Depreciación del periodo': False,
                                    #'Amortización del periodo': False,
                                    'Deterioro': False,
                                    'Otros ingresos y egresos no operativos': False,
                                    'Intereses/gasto financiero': False, 
                                    'Impuestos de renta': False, 
            }
            self.first_time_projecting_object = {'PYG_DEPENDANCE_NAME': 'No aplica', 'COMMENT': 'Proyección primera vez, debug', 'PROJECTION_TYPE': 'Cero' }

            event_dict = event['pathParameters']
            self.id_assessment = event_dict['id_assessment']
            self.projections_found = False
            self.projection_dates = list()
            self.historic_dates = list()
            self.atributes_dict = dict()
            self.years_projected = int()
            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            
            self.create_conection_to_db()
            self.get_raw_pyg()
            self.get_assessment_dates()
            self.update_subs_classifications()
            self.get_projections_information()
            
            self.organize_atributes()
            self.organize_projection_data()

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


    @handler_wrapper('Realizando query de raw_pyg', 'Raw pyg obtenido correctamente', 'Error adquiriendo raw_pyg','Error adquiriendo identificadores de pyg')    
    def get_raw_pyg(self):
        query = "SELECT * FROM RAW_PYG"
        logger.info(f'[get_raw_pyg] Query para obtener los raw pyg: {query}')
        rds_data = self.db_connection.execute(text(query))
        self.master_raw_pyg = {item.ID:item.PYG_ITEM_NAME for item in rds_data.fetchall()}


    @handler_wrapper('Obteniendo las fechas del proceso de valoración', 'Fechas del proceso de valroación obtenidas con exito', 'Error obteniendo las fechas del proceso de valoración', 'Error obteniendo fechas del proceso de valoración')
    def get_assessment_dates(self):
        query = f"SELECT DATES, PROPERTY FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY DATES"
        logger.info(f"[get_assessment_dates] Query a base de datos para obtener las fechas utilizadas en el proces de valoración:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        directory = {'HISTORIC': self.historic_dates, 'PROJECTION': self.projection_dates}
        for date_item in rds_data.fetchall():
            directory.get(date_item.PROPERTY, []).append(date_item.DATES)

        self.historic_dates = [date.strftime('%Y-%m-%d') for date in self.historic_dates]
        if not self.projection_dates:
            logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nAún no existen fechas de proyección')
            return
        self.projection_dates = [date.strftime('%Y') for date in self.projection_dates] #'%Y-%m-%d %H:%M:%S'
        self.projection_dates[0] = self.projection_dates[0] if '-12-' in self.historic_dates[-1] else f'Diciembre {self.projection_dates[0]}' #Se agrega esta linea para que llegue diciembre con formato de anualización a front
        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')

    
    @handler_wrapper('Adquiriendo las clasificaciones actualizadas', 'Clasificaciones de assessment actualizadas con exito', 'Error actualizando clasificaciones de assessment', 'Error comparando clasificaciones de assessment')
    def update_subs_classifications(self):
        #Se mapea cuales clasificaciones existen, para que si desapareció un sub de las clasificaciones, este no sea enviado al front de proyecciones pyg
        query = f"""SELECT B.CLASSIFICATION, A.ANNUALIZED FROM CLASSIFICATION_SUMMARY A, RAW_CLASSIFICATION B, ARCHIVE C
WHERE A.ID_RAW_CLASSIFICATION = B.ID AND C.ID = A.ID_ARCHIVE AND A.ID_ASSESSMENT = :id_assessment ORDER BY C.INITIAL_DATE """
        
        logger.info(f'[get_projections_information] Query para obtener proyecciones del proceso de valoracion:\n{query}')
        rds_data = self.db_connection.execute(text(query), {'id_assessment': self.id_assessment})
        self.historic_data =  [row._asdict() for row in rds_data.fetchall()]
        self.updated_classifications =  set(row['CLASSIFICATION'] for row in self.historic_data)
        for row in self.historic_data:
            row['ANNUALIZED'] = float(row['ANNUALIZED'])
        
    
    @handler_wrapper('Realizando query a proyecciones del proceso de valoracion', 'Proyecciones adquiridas con exito', 'Error adquiriendo proyecciones de pyg', 'Error adquiriendo proyecciones')
    def get_projections_information(self):
        query = f"""SELECT C.PYG_ITEM_NAME, A.ID_DEPENDENCE, A.ORIGINAL_VALUE, A.PROJECTION_TYPE, A.`COMMENT`, B.PROJECTED_DATE, B.ATRIBUTE, B.VALUE, C.IS_SUB
FROM PYG_ITEM A, PROJECTED_PYG B, RAW_PYG C
WHERE A.ID_ASSESSMENT = B.ID_ASSESSMENT AND A.ID_RAW_PYG = B.ID_RAW_PYG AND A.ID_RAW_PYG = C.ID 
AND C.PYG_ITEM_NAME NOT IN {str(self.accounts_to_skip)}
AND A.ID_ASSESSMENT = {self.id_assessment} ORDER BY A.ID_RAW_PYG, B.PROJECTED_DATE"""
        
        logger.info(f'[get_projections_information] Query para obtener proyecciones del proceso de valoracion: {query}')
        rds_data = self.db_connection.execute(text(query))
        projection_data =  [item._asdict() for item in rds_data.fetchall()]
        for item in projection_data:
            self.projections_found = True
            item['value'] = float(item['ORIGINAL_VALUE'])
            item['PYG_DEPENDANCE_NAME'] = self.master_raw_pyg[item['ID_DEPENDENCE']]
            item['ATRIBUTE'] = float(item['ATRIBUTE']) if item['ATRIBUTE'].replace('.', '').replace('-','').isalnum() else ''
            
        self.projection_data = projection_data
        logger.info(f'[get_projections_information] records de proyecciones:\n{projection_data}')


    @handler_wrapper('Acomodando atribute values', 'Atribute values acomodados con exito', 'Error construyendo objetos de atributes de proyeccion anual', 'Error construyendo caracteristicas de proyeccion')
    def organize_atributes(self):
        self.atributes_dict = {classification: {'history': list(), 'projection': list()} for classification in self.updated_classifications.union(set(self.projections_order))}

        for classification in self.atributes_dict:
            history_vector = [row['ANNUALIZED'] for row in self.historic_data if row['CLASSIFICATION'] == classification]
            self.atributes_dict[classification]['history'] = history_vector if history_vector else [0] * len(self.historic_dates)

            projections_vector = [row['ATRIBUTE'] for row in self.projection_data if row['PYG_ITEM_NAME'] == classification]
            self.atributes_dict[classification]['projection'] = projections_vector if projections_vector else [''] * len(self.projection_dates)
        
        for total, properties in self.pyg_totals.items():
            historic_dependance_vectors = self.calculate_total_vector(properties['dependencies'], properties['is_sum'], self.atributes_dict)
            self.atributes_dict[total] = {'history': historic_dependance_vectors}
            self.atributes_dict[total]['projection'] = [row['ATRIBUTE'] for row in self.projection_data if row['PYG_ITEM_NAME'] == total]


    @debugger_wrapper('Error calculando sub total de tabla', 'Error calculando totales de proyecciones de pyg')
    def calculate_total_vector(self, dependencies, vector_signs, search_in):
        for dep in dependencies:
            if dep not in search_in:
                search_in[dep] = {'history': [0] * len(self.historic_dates)}
        dependencies_vectors = [search_in[dependence]['history'] for dependence in dependencies]
        partial_vector = []
        for year_values in zip(*dependencies_vectors):
            year_total = 0
            for index, value in enumerate(year_values):
                year_total = year_total + value * vector_signs[index]
            partial_vector.append(year_total)
        return partial_vector


    @handler_wrapper('Organizando respuesta de proyecciones', 'Respuesta de proyecciones creada con exito', 'Error construyendo objeto final de respuesta', 'Error construyendo objeto de respuesta')   
    def organize_projection_data(self):
        logger.warning(f'[organize_projection_data] Set de proyecciones encontrado:\n{self.updated_classifications}')
        for item_name, is_parent in self.projections_order.items():
            if is_parent:
                subs_to_use = sorted([item for item in self.updated_classifications if item.startswith(item_name) and item != item_name])
                self.organize_found_subs(subs_to_use)
            else:
                item_to_use = next((item for item in self.projection_data if item['PYG_ITEM_NAME'] == item_name ), self.first_time_projecting_object)
                item_to_add = {'account': item_name, 
                'accountProjector': item_to_use['PYG_DEPENDANCE_NAME'], 
                'explication': item_to_use['COMMENT'], 
                'method': item_to_use['PROJECTION_TYPE'], 
                'atributes': self.atributes_dict[item_name]}
                self.partial_response.append(item_to_add)
                

    @debugger_wrapper('Error organizando paquete de subs', 'Error organizando paquete de subs')
    def organize_found_subs(self, subs_package):
        for sub_name in subs_package:
            item_to_use = next((item for item in self.projection_data if item['PYG_ITEM_NAME'] == sub_name ), self.first_time_projecting_object)
            if sub_name not in self.updated_classifications:
                return
            if sub_name in self.updated_classifications:
                item_to_add = {'account': sub_name, 
                'accountProjector': item_to_use['PYG_DEPENDANCE_NAME'], 
                'explication': item_to_use['COMMENT'], 
                'method': item_to_use['PROJECTION_TYPE'], 
                'atributes': self.atributes_dict[sub_name]}
                self.partial_response.append(item_to_add)
                self.updated_classifications.discard(sub_name)
         

    @debugger_wrapper('Error construyendo respuesta final', 'Error construyendo respuesta')
    def response_maker(self, succesfull_run = False, error_str = str()):
        if self.db_connection:
            self.db_connection.close()

        if succesfull_run:
            self.final_response['statusCode'] = 200
            self.final_response['body'] = json.dumps({'year': len(self.projection_dates),'datesHistory': self.historic_dates, 'datesProjections' : self.projection_dates, 'id_assessment': self.id_assessment, 'projection_data': self.partial_response})
            return self.final_response
            
        self.final_response['body'] = json.dumps(self.detailed_raise if self.detailed_raise else error_str)
        return self.final_response




def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    

if __name__ == "__main__":
    event = {"pathParameters": {"id_assessment": "55"}}
    lambda_handler(event, '')

