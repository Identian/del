from sqlalchemy import text
from decorators import handler_wrapper, timing, debugger_wrapper, try_pass_wrapper
import logging
import pandas as pd
import datetime
import sys
import pprint
from models_tables import models_tables

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class orchester_utils_class(object):
    @debugger_wrapper('Error borrando datos de una de las tablas de bd', 'Error sobreescribiendo información')
    def delete_from_bd(self, table, and_clause):
        query = f"DELETE FROM {table} WHERE ID_ASSESSMENT = {self.id_assessment} {and_clause}"
        logger.info(f'[delete_from_bd] Query para borrar posibles datos en {table}:\n{query}')
        self.db_connection.execute(text(query)) #, {'id_assessment': self.id_assessment}
        
    @debugger_wrapper('Error cargando datos a bd', 'Error sobreescribiendo información a bd')
    def df_and_upload(self, records, table_to, and_clause = ''):
        try:
            logger.info(f'[df_and_upload] Cargando datos a la tabla: {table_to}')
            self.delete_from_bd(table_to, and_clause)
            uploading_df = pd.DataFrame.from_records(records)
            uploading_df['ID_ASSESSMENT'] = self.id_assessment
            logger.debug(f'[df_and_upload] Datos que se cargarán a {table_to}:\n{uploading_df.to_string()}')
            uploading_df.to_sql(name = table_to, con=self.db_connection, schema='src_corporate_finance', if_exists='append', index=False)
            logger.info('[df_and_upload] Carga exitosa')
            
        except Exception as e:
            logger.error(f'[df_and_upload] No se pudo cargar información a la tabla {table_to}, motivo:{str(e)}')
            logger.warning(f'[df_and_upload] dataframe que no se pudo cargar a {table_to}:\n{uploading_df.to_string()}')
            self.noting_list.append({'TASK': self.current_context, 'NOTE': f"No se pudo cargar información a la tabla de {table_to}"}) if self.current_context else None
            
        

    @try_pass_wrapper('Error escribiendo step de valoración, posiblemente ya exista')
    def save_assessment_step(self, step):
        if step not in self.steps_list:
            query = 'INSERT ASSESSMENT_STEPS VALUES(:id_assessment, :step)'
            logger.info(f'[save_assessment_step] Query para insertar paso completado del proceso de valoración:\n{query}')
            self.db_connection.execute(text(query), {'id_assessment': self.id_assessment, 'step': step})
                    
    
    @debugger_wrapper('Error cambiando estado del orquestador', 'Error modificando estado')
    def orchestrator_state_update(self, new_status):
        if self.db_connection:
            query = f"""INSERT INTO ORCHESTRATOR_STATUS (ID_ASSESSMENT, STATE) VALUES ({self.id_assessment}, "{new_status}") 
ON DUPLICATE KEY UPDATE STATE=VALUES(STATE), LAST_STATE_TS = CURRENT_TIMESTAMP"""
            logger.info(f'[orchestrator_state_update] query para actualizar el estado del orquestador:\n{query}')
            self.db_connection.execute(text(query)) #, {'id_assessment': self.id_assessment, 'new_status':new_status}
        
        
    @debugger_wrapper('Error obteniendo valor para la cuenta', 'Error obteniendo valor original de una cuenta')    
    def get_historic_account_values(self, account_num):
        found_values = [float(item['value']) for item in self.purged_items if item['account'] == account_num]
        if found_values:
            return found_values
        return [0] * len(self.historic_dates)


    @debugger_wrapper('Error obteniendo valor del summarie', 'Error obteniendo valor original del summarie')    
    def get_historic_summary_values(self, classification, startswith = False):
        if startswith:
            accumulator = {}
            for item in self.summary_data:
                if not item['CLASSIFICATION'].startswith(classification):
                    continue
                if item['id_archive'] not in accumulator:
                    accumulator[item['id_archive']] = item['value']
                else:
                    accumulator[item['id_archive']] = accumulator[item['id_archive']] + item['value']
            found_values = [value for _, value in accumulator.items()]
            
        else:
            found_values = [float(item['value']) for item in self.summary_data if item['CLASSIFICATION'] == classification]
        if found_values:
            return found_values
        return [0] * len(self.historic_dates)
        
    
    @debugger_wrapper('Error calculando sub total de tabla', 'Error calculando totales de proyecciones de pyg')
    def calculate_total_vector(self, dependencies, vector_signs, search_in):
        #logger.info(f'[calculate_total_vector] buscando {dependencies} en {search_in}')
        dependencies_vectors = [search_in[dependence] for dependence in dependencies]
        partial_vector = []
        for year_values in zip(*dependencies_vectors):
            year_total = 0
            for index, value in enumerate(year_values):
                year_total = year_total + value * vector_signs[index]
            partial_vector.append(year_total)
        return partial_vector


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    
def acute(object):
    return f"""{pprint.pformat(object, indent = 4)}"""
    
def is_ok(func, *args, **kw):
    try:
        func(*args, **kw)
        return True
    except Exception:
        return False