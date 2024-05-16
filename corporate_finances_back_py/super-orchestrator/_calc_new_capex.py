
from decorators import handler_wrapper, timing, debugger_wrapper
from dataclasses import dataclass, asdict#, field #field es para cuando se debe crear algo en __post_init__
from dataclasses_box import capex_values_object

import logging
import pandas as pd
import datetime
import sys
from _orchester_utils import acute


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class new_capex_class(object):
    @handler_wrapper('Actualizando datos de nuevo capex', 'Nuevo capex calculado con exito', 'Error actualizando resultados de nuevo capex', 'Error actualizando capex')
    def new_capex_recalc(self):
        if not self.assessment_models.get('MODEL_CAPEX', False):
            logger.warning(f'[new_capex_recalc] No hay atributos para las proyecciones de capex')
            #self.noting_list.append({'TASK': self.current_context, 'NOTE': f"No se halló información de proyeccion de nuevo capex"})
            self.assessment_projections_found = False
            return
        
        self.fixed_assets_proy_vectors = {'acumulated': [], 'period': [], 'asset': []}
        
        self.proyection_mode_directory = {'Proyección automatica': self.proyeccion_auto_new_capex, 'Porcentaje de otra variable': self.percentage_capex_projection, 'Manual': self.manual_projection}
        self.capex_records = list()
        self.capex_values_records = list()
        self.acumulative_capex = [0] * self.projection_dates_len
        self.acumulative_dep_capex = [0] * self.projection_dates_len

        try:
            self.partial_calculator_master()
            self.new_capex_vector = [0] * self.historic_dates_len + self.acumulative_capex
            self.dep_capex = [0] * self.historic_dates_len + self.acumulative_dep_capex
        except:
            self.new_capex_vector = [0] * self.all_dates_long #CAPEX
            self.dep_capex = [0] * self.all_dates_long #Depreciación capex
            
        self.df_and_upload(self.capex_records, 'CAPEX')
        self.df_and_upload(self.capex_values_records, 'CAPEX_VALUES')

    @handler_wrapper('Iniciando master de calculos parciales', 'Master de calculos terminado con éxito', 'Error en el master de calculos de nuevo capex', 'Error calculando nuevo capex')
    def partial_calculator_master(self):
        for partial_capex in  self.assessment_models['MODEL_CAPEX']:
            self.current_capex_name = partial_capex['CAPEX_NAME']
            user_chosen_pyg_row = partial_capex['USED_ACCOUNT_NAME']
            """
            historic_chosen_vector = self.pyg_values_vectors.get(user_chosen_pyg_row, False)
            if not historic_chosen_vector:
                self.noting_list.append({'TASK': self.current_context, 'NOTE': f'No se halló {user_chosen_pyg_row} para el calculo de la linea depreciación nuevo capex'})
                historic_chosen_vector = [0] * self.historic_dates_len
            """
            projected_chosen_vector = self.pyg_projected_vectors.get(user_chosen_pyg_row, False)    
            if not projected_chosen_vector:
                self.noting_list.append({'TASK': self.current_context, 'NOTE': f'No se halló proyeccion de {user_chosen_pyg_row} calculada para calculo de nuevo capex'})
                projected_chosen_vector = [0] * self.projection_dates_len
                
            self.user_chosen_vector = projected_chosen_vector

            self.proyection_mode_directory[partial_capex['METHOD']]()

            self.calculate_historic_new_capex()
            self.new_capex_vector.extend(self.new_capex_projections)
            self.capex_depreciation()
            logger.info(f'[new_capex_recalc] Resultado final de nuevo capex:\n{self.new_capex_vector}\n\nDepreciación capex:\n{self.dep_capex}')
            self.capex_records.append(partial_capex)
            self.acumulative_capex = [i + j for i, j in zip(self.acumulative_capex, self.new_capex_projections)]
            self.acumulative_dep_capex = [i + j for i, j in zip(self.acumulative_dep_capex, self.dep_capex)]
            self.organize_db_records()
               


        

    @handler_wrapper('Calculando historico de nuevo capex', 'Historicos de nuevo capex calculado con exito', 'Error calculando historico de nuevo capex', 'Error calculando historicos de nuevo capex')
    def calculate_historic_new_capex(self):
        """
        operational_income_vector = self.pyg_values_vectors['Ingresos operacionales'] + self.pyg_projected_vectors['Ingresos operacionales']
        #net_asset_vector = [0] + [sum(items) for items in zip(self.user_chosen_vector, self.fixed_assets_proy_vectors['acumulated'])]
        logger.info(f'[calculate_historic_new_capex] Vectores con los que se calcula net_asset_vector:\noperational_income_vector: {operational_income_vector}\nself.fixed_assets_proy_vectors.acumulated:{self.fixed_assets_proy_vectors["acumulated"]}')
        net_asset_vector = [0] + [sum(items) for items in zip(operational_income_vector, self.fixed_assets_proy_vectors['acumulated'])]
        self.new_capex_vector = []
        for index, _ in enumerate(self.historic_dates):
            self.new_capex_vector.append(net_asset_vector[index+1] - net_asset_vector[index] + self.fixed_assets_proy_vectors['period'][index])
            #self.new_capex_vector.append(0)
        """
        """
        self.fixed_assets_proy_vectors['net_value']
        self.new_capex_vector = [0]
        for index, _ in enumerate(self.historic_dates[1:]):
            self.new_capex_vector.append(self.fixed_assets_proy_vectors['net_value'][index+1] - self.fixed_assets_proy_vectors['net_value'][index] + self.fixed_assets_proy_vectors['period'][index + 1])

        logger.info(f"[calculate_historic_new_capex] historicos de nuevo capex: {self.new_capex_vector}")
        """
        self.new_capex_vector = [0] * self.historic_dates_len

    @handler_wrapper('Calculando proyecciones automaticas de nuevo capex', 'Proyecciones de nuevo capex calculadas con exito', 'Error calculando proyecciones automaticas de nuevo capex', 'Error calculando proyecciones de nuevo capex')
    def proyeccion_auto_new_capex(self):
        #Este metodo no se debería estar usando
        try:
            logger.info(f'[proyeccion_auto_new_capex] información requerida para sacar calcular proyecciones de nuevo capex:\nself.new_capex_vector: {self.new_capex_vector}\nself.user_chosen_vector: {self.user_chosen_vector}')
            self.new_capex_projections = [self.new_capex_vector[-1] / self.user_chosen_vector[self.historic_dates_len -1] * self.user_chosen_vector[self.historic_dates_len]]
            for index, user_chosen_value in enumerate(self.user_chosen_vector[self.historic_dates_len + 1:], start = self.historic_dates_len + 1):
                self.new_capex_projections.append( user_chosen_value + self.user_chosen_vector[index - 1])
            
        except:
            self.new_capex_projections = [0] * self.projection_dates_len
            self.noting_list.append({'TASK': self.current_context, 'NOTE':'Error calculando nuevo capex, esposible que la cuenta dependiente haya desaparecido o haya sido proyectada a cero'})
        logger.info(f'[proyeccion_auto_new_capex] resultado CAPEX: {self.new_capex_projections}')
 

    @debugger_wrapper('Error calculando proyecciones de tipo porcentual de capex parcial', 'Error calculando proyecciones porcentuales de capex parcial')
    def percentage_capex_projection(self):
        manual_capex_atributes = [ item['MANUAL_PERCENTAGE'] for item in self.assessment_models['MODEL_CAPEX_VALUES'] if item ['CAPEX_NAME'] == self.current_capex_name][-self.projection_dates_len:]
        manual_capex_atributes = [float(item) for item in manual_capex_atributes]
        self.new_capex_projections = [i*j/100 for i,j in zip(self.user_chosen_vector, manual_capex_atributes)]


    @debugger_wrapper('Error calculando proyecciones manuales de nuevo capex', 'Error calculando proyecciones de capex parcial')
    def manual_projection(self):
        self.new_capex_projections = [ item['MANUAL_PERCENTAGE'] for item in self.assessment_models['MODEL_CAPEX_VALUES'] if item ['CAPEX_NAME'] == self.current_capex_name][-self.projection_dates_len:]
        self.new_capex_projections = [float(item) for item in self.new_capex_projections]


    @handler_wrapper('Calculando depreciación de capex', 'Depreciación de capex calculada con exito', 'Error calculando depreciación de capex', 'Error calculando depreciación de capex')
    def capex_depreciation(self):
        dep_years = self.assessment_models['MODEL_CAPEX'][0]['PERIODS']
        dep_matrix = [[0] * len(self.new_capex_projections) for _ in self.new_capex_projections]
        for index, value in enumerate(self.new_capex_projections):
            yearly_dep_vector = value / dep_years
            for year in range(dep_years):
                try:
                    dep_matrix[index][index+1+year] = yearly_dep_vector
                except:
                    break
        logger.warning(f'[capex_depreciation] Matrix de depreciación capex:\n{dep_matrix}')
        self.dep_capex = [0] * self.historic_dates_len + [sum(items) for items in zip(*dep_matrix)]


    @handler_wrapper('Organizando records para envio a bd', 'Records de capex organizados con exito', 'Error organizando records de capex', 'Error guardando datos en bd')
    def organize_db_records(self):
        manual_capex_atributes = [ item['MANUAL_PERCENTAGE'] for item in self.assessment_models['MODEL_CAPEX_VALUES'] if item ['CAPEX_NAME'] == self.current_capex_name][-self.projection_dates_len:]
        self.capex_values_records.extend([ asdict(capex_values_object(*args)) for args in zip([self.current_capex_name] * self.historic_dates_len, self.historic_dates_long, [''] * self.historic_dates_len, self.new_capex_vector, self.dep_capex)]) #sí acá me toca cambiar la forma de las fechas debo inyectarlo manualmente
        self.capex_values_records.extend([ asdict(capex_values_object(*args)) for args in zip([self.current_capex_name] * self.projection_dates_len, self.projection_dates_long, manual_capex_atributes, self.new_capex_vector[self.historic_dates_len:], self.dep_capex[self.historic_dates_len:])])
    
        
def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)