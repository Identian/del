
from decorators import handler_wrapper, debugger_wrapper
from sqlalchemy import text
import json
import logging
import sys
import pandas as pd
import datetime
import os
from utils import get_secret, connect_to_db, call_dynamic_engine, connect_to_db_local_dev


logging.basicConfig()
logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(module)s] %(message)s', force=True, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    sc_obj = script_object(event)
    lambda_response = sc_obj.starter()
    logger.info(f'respuesta final de lambda: \n{lambda_response}')
    return lambda_response


class script_object:

    def __init__(self, event) -> None:
        try:
            self.failed_init = False
            self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 
                                                'Access-Control-Allow-Origin': '*', 
                                                'Access-Control-Allow-Methods': '*'}, "statusCode": 500, 'body': {}}
            self.db_connection = 0
            self.detailed_raise = ''
            self.partial_response = []

            logger.warning(f'event de entrada: {str(event)}')
            event_body_dict = json.loads(event['body'])
            self.years = event_body_dict["year"]
            self.historic_dates = []
            self.projection_dates = event_body_dict["datesProjections"]
            self.projection_dates_len = len(self.projection_dates)
            self.projected_items = event_body_dict["projection_data"]
            self.id_assessment = event_body_dict["id_assessment"]

            self.master_raw_pyg = dict()
            
            self.nan_years = [float('NaN')]*self.years
            self.projections_to_db_df = pd.DataFrame()
            self.pyg_items_to_db_df = pd.DataFrame()

        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            logger.info("iniciando tareas de lambda")
            self.create_conection_to_db()
            self.get_assessment_dates()
            self.get_raw_pyg()
            self.delete_earlier_pyg_items_in_bd()

            self.organize_bd_projections_data()
            self.organize_pyg_items()
            self.organize_projection_dates_to_df()
            
            self.upload_projections_df()
            self.pyg_items_to_db()
            self.save_projection_dates()

            self.save_assessment_step()
            call_dynamic_engine(self.id_assessment, __name__)
    
            logger.info("Tareas de lambda finalizadas con exito")
            return self.response_maker(succesfull=True)
        except Exception as e:
            logger.error(
                f"[starter] Error critico de lambda en el comando de la linea {get_current_error_line()}, motivo: {str(e)}")
            return self.response_maker(exception_str = str(e))


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


    @handler_wrapper('Obteniendo las fechas del proceso de valoración', 'Fechas del proceso de valroación obtenidas con exito', 'Error obteniendo las fechas del proceso de valoración', 'Error obteniendo fechas del proceso de valoración')
    def get_assessment_dates(self):
        query = f"SELECT DATES, PROPERTY FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY DATES"
        logger.info(f"[get_assessment_dates] Query a base de datos para obtener las fechas utilizadas en el proces de valoración:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        directory = {'HISTORIC': self.historic_dates}
        for date_item in rds_data.fetchall():
            directory.get(date_item.PROPERTY, []).append(date_item.DATES) #Las self.projection_dates no las estoy usando para nada

        projection_dates_temp = []
        assessment_date = self.historic_dates[-1]
        for i in range(1, self.projection_dates_len+1):
            projection_dates_temp.append(f'{assessment_date.year + i}-01-01')
            
        if '-12-' not in assessment_date.strftime(f'%Y-%m-%d'):
            projection_dates_temp = [f'{assessment_date.year}-12-01'] + projection_dates_temp[:-1]
        self.projection_dates = projection_dates_temp
        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')

    
    @handler_wrapper('Realizando query de raw_pyg', 'Raw pyg obtenido correctamente', 'Error adquiriendo raw_pyg','Error adquiriendo identificadores de pyg')    
    def get_raw_pyg(self):
        query = "SELECT * FROM RAW_PYG"
        logger.info(f'[get_raw_pyg] Query para obtener los raw pyg: {query}')
        rds_data = rds_data = self.db_connection.execute(text(query), {"id_assessment":self.id_assessment})
        self.master_raw_pyg = {item.PYG_ITEM_NAME:item.ID for item in rds_data.fetchall()}
    

    @handler_wrapper('Chequeando si ya hay Proyecciones en BD', 'Chequeo terminado de proyecciones en BD terminado', 'Error chequeando si ya hay proyecciones en BD', 'Errores verificando proyecciones en BD')
    def delete_earlier_pyg_items_in_bd(self):
        query = "DELETE FROM PYG_ITEM WHERE ID_ASSESSMENT = :id_assessment"
        logger.warning(f'[delete_earlier_pyg_items_in_bd] Query para eliminacion de pyg items: \n{query}')
        self.db_connection.execute(text(query), {"id_assessment":self.id_assessment})

        query = "DELETE FROM PROJECTED_PYG WHERE ID_ASSESSMENT = :id_assessment"
        logger.warning(f'[delete_earlier_pyg_items_in_bd] Query para eliminacion de pyg proyectados: \n{query}')
        self.db_connection.execute(text(query), {"id_assessment":self.id_assessment})
        
        query = "DELETE FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = :id_assessment AND PROPERTY =\"PROJECTION\" "
        logger.info(f'[delete_earlier_pyg_items_in_bd] Query para eliminar fechas de proyeccion existentes del proceso de valoración: \n{query}')
        self.db_connection.execute(text(query), {"id_assessment":self.id_assessment})

 
    @handler_wrapper('Organizando informacion de proyecciones a BD', 'Información organizada con exito', 'Error organizando informacion a bd', 'Error organizando datos que se subiran a bd')
    def organize_bd_projections_data(self):
        to_db_item_list = []
        
        for item in self.projected_items:
            item_name_id = self.master_raw_pyg[item['account']]
            for index, proyection_date in enumerate(self.projection_dates):
                try:
                    to_db_item = {'ID_RAW_PYG':item_name_id, 'PROJECTED_DATE': proyection_date, 'VALUE': 0, 'ATRIBUTE':item['atributes'].get('projection',self.nan_years)[index]}
                except:
                    print('hola')
                to_db_item_list.append(to_db_item)
        self.projections_to_db_df = pd.DataFrame.from_records(to_db_item_list)
        self.projections_to_db_df['ID_ASSESSMENT'] = self.id_assessment
        self.projections_to_db_df.fillna(value= {'ATRIBUTE': ''}, inplace = True)


    @handler_wrapper('Organizando items de pyg', 'Items de pyg organizados con exito', 'Error organizando items de pyg', 'Error organizando resultados del pyg actual para carga en db')
    def organize_pyg_items(self):
        pyg_items_list = []
        for item in self.projected_items:
            if item['method'] in ["Porcentaje de otra variable", "Tasas impositivas"]:
                this_item_dependence_id = self.master_raw_pyg[item['accountProjector']]
            else:
                this_item_dependence_id = self.master_raw_pyg['No aplica']
            this_item_raw_pyg_id = self.master_raw_pyg[item['account']]
            this_item_made = {'ID_RAW_PYG': this_item_raw_pyg_id, 
                                'ID_DEPENDENCE': this_item_dependence_id, 
                                'ORIGINAL_VALUE': 0, 
                                'PROJECTION_TYPE': item['method'], 
                                'COMMENT': item.get('explication', '')}
            pyg_items_list.append(this_item_made)
        self.pyg_items_to_db_df = pd.DataFrame.from_records(pyg_items_list)
        self.pyg_items_to_db_df['ID_ASSESSMENT'] = self.id_assessment
        logger.info(f'[organize_pyg_items] Dataframe de items de pyg a subir: \n{self.pyg_items_to_db_df}')

    
    @handler_wrapper('Organizando fechas historicas para cargar en bd', 'Fechas historicas construidas a dataframe', 'Error organizando fechas historicas a dataframe', 'Error manipulando fechas historicas para guardado en bd')    
    def organize_projection_dates_to_df(self):
        self.projection_dates_to_db = [datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S') for date in self.projection_dates]
        self.projection_dates_df = pd.DataFrame({'DATES': self.projection_dates_to_db, 
                                                    'ID_ASSESSMENT': [self.id_assessment] * self.projection_dates_len, 
                                                    'PROPERTY': ['PROJECTION']* self.projection_dates_len})
        

    @handler_wrapper('Subiendo dataframe de proyecciones a bd', 'Carga de datos exitosa', 'Error en la carga de datos', 'Error cargando datos de proyecciones a bd')
    def upload_projections_df(self):
        logger.info(f'[upload_projections_df] Dataframe de proyecciones a subir: \n{self.projections_to_db_df}')
        self.projections_to_db_df.to_sql(name='PROJECTED_PYG', con=self.db_connection, if_exists='append', index=False)


    @handler_wrapper('cargando items de pyg a db', 'items de pyg cargados con exito', 'Error cargando los items de pyg', 'Error cargando los items de pyg a bd, es posible que las proyecciones sí se hayan cargado con exito')
    def pyg_items_to_db(self):
        logger.info(f'[pyg_items_to_db] Dataframe de pyg_items a subir: \n{self.pyg_items_to_db_df.to_string()}')
        self.pyg_items_to_db_df.to_sql(name='PYG_ITEM', con=self.db_connection, if_exists='append', index=False)

            
    @handler_wrapper('Guardando fechas historicas del proceso de valoración en bd','fechas historicas guardadadas con exito','Error guardando fechas historicas en bd', 'Error guardando fechas historicas en bd')
    def save_projection_dates(self):
        logger.info(f'[save_historic_dates] Dataframe de fechas que se va a guardar en bd:\n{self.projection_dates_df.to_string()}')
        self.projection_dates_df.to_sql(name='ASSESSMENT_DATES', con=self.db_connection, if_exists='append', index=False)
                
    
    @handler_wrapper('Guardando el paso del proceso de valoracion','Paso guardado correctamente','Error guardando el paso del proceso de valoración', 'Error guardando informacion')
    def save_assessment_step(self):
        try:
            query = "INSERT INTO ASSESSMENT_STEPS VALUES (:id_assessment, \"PYG\");"
            logger.info(f"[save_assessment_step] Query a base de datos para guardar el paso del proceso de valoracion: \n{query}")
            self.db_connection.execute(text(query), {"id_assessment":self.id_assessment})
        except Exception as e:
            logger.warning(f'[save_assessment_step] Es posible que el step del proceso de valoracion ya haya sido guardado, sin embargo, este es el mensaje de error:\n{str(e)}')

    
    def response_maker(self, succesfull=False, exception_str=""):
        if self.db_connection:
            self.db_connection.close()
        if not succesfull:
            self.final_response["body"] = json.dumps(exception_str)
        else:
            self.final_response["statusCode"] = 200
            self.final_response["body"] = json.dumps("ok")
        return self.final_response


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)


if __name__ == "__main__":
    #event = {'body': "{\"id_assessment\":55,\"projections\":{\"year\":3,\"dates_projection\":[\"2024\",\"2025\",\"2026\"],\"projection_data\":[{\"account\":\"Ingresos operacionales 1\",\"accountProjector\":\"Seleccione\",\"method\":\"Valor constante\",\"atributes\":[\"\",\"\",\"\"],\"explication\":\"a\"},{\"account\":\"Costos (Sin depreciación)\",\"accountProjector\":\"Seleccione\",\"method\":\"Valor constante\",\"atributes\":[\"\",\"\",\"\"],\"explication\":\"a\"},{\"account\":\"Gastos operacionales 1\",\"accountProjector\":\"Seleccione\",\"method\":\"Valor constante\",\"atributes\":[\"\",\"\",\"\"],\"explication\":\"a\"},{\"account\":\"Otros ingresos y egresos operativos\",\"accountProjector\":\"Seleccione\",\"method\":\"Valor constante\",\"atributes\":[\"\",\"\",\"\"],\"explication\":\"a\"},{\"account\":\"Depreciación del periodo\",\"accountProjector\":\"Seleccione\",\"method\":\"Valor constante\",\"atributes\":[\"\",\"\",\"\"],\"explication\":\"a\"},{\"account\":\"Amortización del periodo\",\"accountProjector\":\"Seleccione\",\"method\":\"Valor constante\",\"atributes\":[\"\",\"\",\"\"],\"explication\":\"a\"},{\"account\":\"Deterioro\",\"accountProjector\":\"Seleccione\",\"method\":\"Valor constante\",\"atributes\":[\"\",\"\",\"\"],\"explication\":\"a\"},{\"account\":\"Otros ingresos y egresos no operativos\",\"accountProjector\":\"Seleccione\",\"method\":\"Valor constante\",\"atributes\":[\"\",\"\",\"\"],\"explication\":\"a\"},{\"account\":\"Intereses/gasto financiero\",\"accountProjector\":\"Seleccione\",\"method\":\"Valor constante\",\"atributes\":[\"\",\"\",\"\"],\"explication\":\"a\"},{\"account\":\"Impuestos de renta\",\"accountProjector\":\"Seleccione\",\"method\":\"Valor constante\",\"atributes\":[\"\",\"\",\"\"],\"explication\":\"a\"}]}}"}
    event = {'body': '{"id_assessment":"2055","year":4,"datesHistory":["2021-12-31","2022-12-31","2023-03-23"],"datesProjections":["Diciembre 2023","2024","2025","2026","2027"],"projection_data":[{"account":"Ingresos operacionales 1","accountProjector":"Seleccione","method":"Tasa de crecimiento fija","atributes":{"history":[35160.28,43801.61,42685.68],"projection":[10,10,10,10,10]},"explication":"A"},{"account":"Costos (Sin depreciación)","accountProjector":"Seleccione","method":"Tasa de crecimiento fija","atributes":{"history":[0,0,0],"projection":[10,10,10,10,10]},"explication":"a"},{"account":"Gastos operacionales 1","accountProjector":"Seleccione","method":"Valor constante","atributes":{"history":[26595.48,28935.46,24025.02],"projection":["","","","",""]},"explication":"a"},{"account":"Otros ingresos y egresos operativos","accountProjector":"Seleccione","method":"Valor constante","atributes":{"history":[0,0,0],"projection":["","","","",""]},"explication":"a"},{"account":"Deterioro","accountProjector":"Seleccione","method":"Valor constante","atributes":{"history":[0,0,0],"projection":["","","","",""]},"explication":"a"},{"account":"Otros ingresos y egresos no operativos","accountProjector":"Seleccione","method":"Valor constante","atributes":{"history":[851.6299999999999,2024.0300000000002,6526.65],"projection":["","","","",""]},"explication":"a"},{"account":"Intereses/gasto financiero","accountProjector":"Seleccione","method":"Valor constante","atributes":{"history":[0,0,0],"projection":["","","","",""]},"explication":"a"},{"account":"Impuestos de renta","accountProjector":"Seleccione","method":"Valor constante","atributes":{"history":[0,0,0],"projection":["","","","",""]},"explication":"a"}]}'}
    lambda_handler(event, '')