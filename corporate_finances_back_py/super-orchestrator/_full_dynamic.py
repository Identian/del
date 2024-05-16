from sqlalchemy import text
from decorators import handler_wrapper, timing, debugger_wrapper
import logging
import pandas as pd
import datetime
import sys
from models_tables import simple_atributes_tables, ordered_atributes_tables
from dataclasses import dataclass, asdict#, field #field es para cuando se debe crear algo en __post_init__


logger = logging.getLogger()
logger.setLevel(logging.INFO)

    
    
class dynamic_class(object):
    @debugger_wrapper('Error en starter de recurrence_class', 'Error en master de recurrencia')
    def full_dynamic_starter(self):
        self.get_assessment_dates()
        self.acquire_assessment_simple_attributes()
        self.acquire_assessment_ordered_attributes()

    @handler_wrapper('Obteniendo las fechas del proceso de valoración', 'Fechas del proceso de valroación obtenidas con exito', 'Error obteniendo las fechas del proceso de valoración', 'Error obteniendo fechas del proceso de valoración')
    def get_assessment_dates(self):
        query = "SELECT DATES, PROPERTY FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = :id_assessment ORDER BY DATES"
        logger.info(f"[get_assessment_dates] Query a base de datos para obtener las fechas utilizadas en el proces de valoración:\n {query}")
        rds_data = self.db_connection.execute(text(query), {"id_assessment":self.id_assessment})
        directory = {'HISTORIC': self.historic_dates, 'PROJECTION': self.projection_dates}
        for date_item in rds_data.fetchall():
            directory.get(date_item.PROPERTY, []).append(date_item.DATES)

        #self.historic_dates = [date.strftime('%Y-%m-%d %H:%M:%S') for date in self.historic_dates]
        self.projection_dates = [date.strftime('%Y-%m-%d') for date in self.projection_dates]
        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')

    @handler_wrapper('Obteniendo Atributos simples del proceso de valoración', 'Atributos simples del proceso de valoración obtenidos con exito', 'Error obteniendo los atributos simples del proceso de valoración', 'Error obteniendo atributos del proceso de valoración')
    def acquire_assessment_simple_attributes(self):
        for table, columns in simple_atributes_tables.items():
            cleaned_columns = str(columns).replace('[','').replace(']','').replace("'", '')
            query = f"SELECT {cleaned_columns} FROM {table} WHERE ID_ASSESSMENT = :id_assessment"
            logger.info(f"[acquire_assessment_simple_attributes] Query a base de datos para obtener los atributos de la tabla {table}:\n{query}")
            rds_data = self.db_connection.execute(text(query), {"id_assessment":self.id_assessment})
            self.assessment_models[f'MODEL_{table}'] = [row._asdict() for row in rds_data.fetchall()]
            logger.warning(f'[acquire_assessment_simple_attributes] Modelo hallado para la tabla {table}:\n{self.assessment_models[f"MODEL_{table}"]}')


    @handler_wrapper('Obteniendo Atributos del proceso de valoración', 'Atributos del proceso de valoración obtenidos con exito', 'Error obteniendo atributos del proceso de valoración', 'Error obteniendo atributos del proceso de valoración')
    def acquire_assessment_ordered_attributes(self):
        for table, cols_order in ordered_atributes_tables.items():
            cleaned_columns = str(cols_order['columns']).replace('[','').replace(']','').replace("'", '')
            query = f"SELECT {cleaned_columns} FROM {table} WHERE ID_ASSESSMENT = :id_assessment ORDER BY {cols_order['order']}"
            logger.info(f"[acquire_assessment_ordered_attributes] Query a base de datos para obtener los atributos de la tabla {table}:\n{query}")
            rds_data = self.db_connection.execute(text(query), {"id_assessment":self.id_assessment})
            self.assessment_models[f'MODEL_{table}'] = [row._asdict() for row in rds_data.fetchall()]
            logger.warning(f'[acquire_assessment_ordered_attributes] Modelo hallado para la tabla MODEL_{table}:\n{self.assessment_models[f"MODEL_{table}"]}')
        
        


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)