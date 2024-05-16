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
            self.partial_response = {}
    
            logger.warning(f'[__init__] Event de entrada:\n{str(event)}')
            
            event_dict = event['pathParameters']
            self.id_assessment = event_dict['id_assessment']
                                    
            self.pyg_front_items = ["Ingresos operacionales",
                                    "Costos (Sin depreciación)",
                                    "Utilidad bruta",
                                    "Gastos operacionales",
                                    "Otros ingresos y egresos operativos",
                                    "EBITDA",
                                    "Depreciación Capex",
                                    "Depreciación del Periodo",
                                    "Amortización del Periodo",
                                    "Deterioro",
                                    "EBIT",
                                    "Otros ingresos y egresos no operativos",
                                    "Intereses/gasto financiero",
                                    "Utilidad antes de impuestos",
                                    "Impuestos de renta",
                                    "Utilidad neta"]
            self.pyg_parents = ['Ingresos operacionales', 'Gastos operacionales']
            
            self.historic_dates = list()
            self.projection_dates = list()
            self.total_asssessment_dates = 0
            self.found_subs = list()
            self.parents_sub_relations = dict()
            self.pyg_rows_vectors = dict()
            
            
            

        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            
            self.create_conection_to_db()
            self.get_assessment_dates()
            self.get_pyg_results()
            self.relate_parents_with_subs()
            self.create_row_vectors()
            self.organize_final_response()

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
            directory.get(date_item.PROPERTY, []).append(date_item.DATES)
            self.total_asssessment_dates = self.total_asssessment_dates +1

        self.historic_dates = [date.strftime('%Y-%m-%d') for date in self.historic_dates]
        self.projection_dates = [date.strftime('%Y') for date in self.projection_dates] #'%Y-%m-%d %H:%M:%S'
        try:
            self.projection_dates[0] = self.projection_dates[0] if '-12-' in self.historic_dates[-1] else f'Diciembre {self.projection_dates[0]}' #Se agrega esta linea para que llegue diciembre con formato de anualización a front
        except:
            pass
        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')


    @handler_wrapper('Obteniendo los resultados calculados de pyg', 'Resultados calculados de pyg obtenidos con exito', 'Error obteniendo resultados de pyg', 'Error obteniendo resultados de pyg')
    def get_pyg_results(self):
        query = f"""SELECT B.PYG_ITEM_NAME AS name, B.IS_SUB, A.VALUE AS value, A.HINT FROM PYG_RESULTS A, RAW_PYG B 
WHERE A.ID_RAW_PYG = B.ID AND A.ID_ASSESSMENT = {self.id_assessment} ORDER BY A.DATES"""

        logger.info(f"[get_pyg_results] Query a base de datos para obtener los resultados de pyg del proceso de valoración:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        self.pyg_data = [row._asdict() for row in rds_data.fetchall()]
        for row in self.pyg_data:
            row['value'] = float(row['value'])
            if row['IS_SUB'] and row['name'] not in self.found_subs:
                self.found_subs.append(row['name'])
            

    @handler_wrapper('Relacionando cuentas padre con subs hallados', 'Relación construída', 'Error creando relación de cuentas padres con subs', 'Error analizando sub cuentas')
    def relate_parents_with_subs(self):
        for parent in self.pyg_parents:
            self.parents_sub_relations[parent] = [sub for sub in self.found_subs if sub.startswith(parent)]
    
    
    @handler_wrapper('Creando vectores para todos los resultados obtenidos de pyg', 'Vectores creados con exito', 'Error creando vectores de resultados', 'Error creando tabla de resultados')
    def create_row_vectors(self):
        current_all_rows = self.pyg_front_items + self.found_subs
        for pyg_row in current_all_rows:
            self.pyg_rows_vectors[pyg_row] = [{'value':item['value'], 'hint': item['HINT']} for item in self.pyg_data if item['name'] == pyg_row]
        
        dep_capex_vector =  [item['value'] for item in self.pyg_data if item['name'] == "Depreciación Capex"]
        logger.warning(f'[mira aca] {dep_capex_vector}')
        if not any(dep_capex_vector):
            self.shadowing_capex_dependant_projections()
            del self.pyg_rows_vectors["Depreciación Capex"]
            self.pyg_front_items.remove("Depreciación Capex")
            

    @handler_wrapper('Ocultando proyecciones dependientes activos fijos y capex', 'Proyecciones ocultadas con éxito', 'Error ocultando proyecciones dependientes', 'Error ocultando resultados dependientes de capex')
    def shadowing_capex_dependant_projections(self):
        for pyg_row in ["Depreciación Capex", "Depreciación del Periodo", "Amortización del Periodo"]:
            self.pyg_rows_vectors[pyg_row] = self.pyg_rows_vectors[pyg_row][:len(self.historic_dates)] + [-1] * len(self.projection_dates)
            #del self.pyg_rows_vectors[pyg_row]
            #del self.pyg_front_items[self.pyg_front_items.index(pyg_row)]


    @handler_wrapper('Organizando respuesta final', 'Respuesta final organizada satisfactoriamente', 'Error organizando respuesta final', 'Error organizando respuesta de servicio')
    def organize_final_response(self):
        data = []
        for pyg_row in self.pyg_front_items:
            if pyg_row in self.parents_sub_relations:
                partial_parent_data = self.micro_pyg_object_creator(pyg_row)
                partial_parent_data['subs'] = []
                for sub in self.parents_sub_relations[pyg_row]:
                    partial_parent_data['subs'].append(self.micro_pyg_object_creator(sub))
                data.append(partial_parent_data)
            else:
                data.append(self.micro_pyg_object_creator(pyg_row))

        self.partial_response = {'datesHistory': self.historic_dates, 'datesProjection': self.projection_dates, 'data': data}


    @debugger_wrapper('Error creando una linea de pyg', 'Error creando una de las lineas de pyg')
    def micro_pyg_object_creator(self, pyg_row):
        if not self.projection_dates:
            return {'name': pyg_row, 'values': {'history': self.pyg_rows_vectors[pyg_row][:len(self.historic_dates)], 'projection': []}}
        return {'name': pyg_row, 'values': {'history': self.pyg_rows_vectors[pyg_row][:len(self.historic_dates)], 'projection': self.pyg_rows_vectors[pyg_row][-1 * len(self.projection_dates):]}}



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
    

if __name__ == "__main__":
    event = {"pathParameters": {"id_assessment": "2049"}}
    lambda_handler(event, '')