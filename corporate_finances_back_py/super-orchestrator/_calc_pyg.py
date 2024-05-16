from decorators import handler_wrapper, timing, debugger_wrapper
import logging
import pandas as pd
import datetime
import sys
from dataclasses import dataclass, asdict#, field #field es para cuando se debe crear algo en __post_init__

@dataclass
class pyg_results_object:
    ID_RAW_PYG  : str
    DATES       : str
    VALUE       : float
    HINT        : float


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class pyg_class(object):
    @handler_wrapper('Actualizando datos iniciales de pyg', 'pyg primera parte calculada', 'Error calculando primera parte de pyg', 'Error actualizando pyg')
    def pyg_first_half_recalc(self):
        pyg_simple_calculations_A = ['Ingresos operacionales',
                            'Costos (Sin depreciación)',
                            'Gastos operacionales',
                            'Otros ingresos operativos',
                            'Otros egresos operativos',
                            'Deterioro',
                            'Impuestos de renta',
                            'Intereses/gasto financiero',
                            'Otros ingresos no operativos',
                            'Otros egresos no operativos','Depreciación del Periodo', 'Amortización del Periodo']

        #TODO: ver en vscode si a esta variable le puedo cambiar el nombre fácil, algo self.current_pyg_totals_A
        self.current_pyg_totals = { 'Ingresos operacionales': {'dependencies': [], 'is_sum': []},
                                      'Gastos operacionales': {'dependencies': [], 'is_sum': []},
                       'Otros ingresos y egresos operativos': {'dependencies':['Otros ingresos operativos','Otros egresos operativos'],'is_sum':[1,-1]},
                    'Otros ingresos y egresos no operativos': {'dependencies': ['Otros ingresos no operativos','Otros egresos no operativos'], 'is_sum': [1,-1]},
                                            'Utilidad bruta': {'dependencies': ['Ingresos operacionales','Costos (Sin depreciación)'],'is_sum': [1, -1]},
                                                    'EBITDA': {'dependencies': ['Utilidad bruta','Gastos operacionales','Otros ingresos y egresos operativos'],'is_sum': [1, -1, 1]}}
                                                
                                                
        for classification in pyg_simple_calculations_A:
            classification_value_vector, classification_hint_vector = self.filter_classification(classification)
            self.pyg_values_vectors[classification] = classification_value_vector
            self.pyg_hints_vectors[classification] = classification_hint_vector

        parents_accounts = sorted(set(item['CLASSIFICATION'] for item in self.summary_data if item['IS_PARENT']))
        logger.warning(f'[pyg_first_half_recalc] parents_accounts encontrados: \n{parents_accounts}')
        
        for parent in parents_accounts:
            sons_of = self.filter_parents_of(parent)
            self.current_pyg_totals[parent]['dependencies'] = sons_of
            self.current_pyg_totals[parent]['is_sum'] = [1] * len(sons_of)
            
        for total_partial, total_dict in self.current_pyg_totals.items():
            self.calculate_partial_totals(total_partial, total_dict, self.historic_dates_len)
            
            
        del self.current_pyg_totals['Otros ingresos y egresos operativos']
        del self.current_pyg_totals['Otros ingresos y egresos no operativos']
        for classification in ['Otros ingresos operativos', 'Otros egresos operativos', 'Otros ingresos no operativos', 'Otros egresos no operativos']:
            del self.pyg_values_vectors[classification]

        logger.warning(f'[pyg_first_half_recalc] Resultados pyg post calculadora\n**Vector de valores por clasificacion**\n{self.pyg_values_vectors}\n\n**Vector de hints por clasificacion**\n{self.pyg_hints_vectors}') 
        

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
        logger.warning(f'[filter_parents_of] Agregando values y hints de la clasificacion padre {parent}, clasificaciones hijas encontradas:\n{found_sons_classifications}')
        for son_classification in found_sons_classifications: 
            classification_value_vector, classification_hint_vector = self.filter_classification(son_classification)
            self.pyg_values_vectors[son_classification] = classification_value_vector
            self.pyg_hints_vectors[son_classification] = classification_hint_vector
        return found_sons_classifications


    #TODO: tengo 3 totalizadores en total para pyg, depronto usando solo uno y agregandolo en el loop de proyecciones, pueda sacar más fácil esta pantalla
    @handler_wrapper('Calculando totales parciales', 'Totales parciales calculadas', 'Error calculando totales parciales', 'No se pudieron calcular "Otros ingresos y egresos"')
    def calculate_partial_totals(self, partial_total, dependencies_dict, hint_length):
        self.pyg_values_vectors[partial_total] = self.calculate_total_vector(dependencies_dict['dependencies'], dependencies_dict['is_sum'], self.pyg_values_vectors)
        self.pyg_hints_vectors[partial_total] = ['+ '.join(dependencies_dict['dependencies'])] * hint_length
        logger.debug(f'[calculate_partial_totals] El vector de totales de {partial_total} obtenido es\n{self.pyg_values_vectors[partial_total]} a partir de las dependencias:{dependencies_dict}')
        
        
    @handler_wrapper('Actualizando datos finales de pyg', 'pyg segunda parte calculada', 'Error calculando parte final de pyg', 'Error actualizando pyg')
    def pyg_second_half_recalc(self):
        self.current_pyg_totals.update({                          'EBIT':{'dependencies': ['EBITDA','Depreciación del Periodo','Amortización del Periodo','Deterioro', 'Depreciación Capex'], 'is_sum':[1,-1,-1,-1,-1]},
                                           'Utilidad antes de impuestos':{'dependencies': ['EBIT','Otros ingresos y egresos no operativos','Intereses/gasto financiero'], 'is_sum':[1,1,-1]},
                                                         'Utilidad neta':{'dependencies': ['Utilidad antes de impuestos','Impuestos de renta'], 'is_sum':[1,-1]}})
            
            
        if self.capex_summary_records:
            self.pyg_projected_vectors['Depreciación Capex'] = self.dep_capex[self.historic_dates_len:]
            for name in ['Depreciación Capex', 'Depreciación del Periodo', 'Amortización del Periodo']:
                self.pyg_values_vectors[name] = self.capex_summary_vectors.get(name, self.dep_capex)
                self.pyg_hints_vectors[name] = ['Modificado desde Capex'] * self.all_dates_len
                
                self.pyg_projected_vectors[name] = self.capex_summary_vectors.get(name, self.dep_capex)[self.historic_dates_len:]
                
                logger.info(f'[pyg_second_half_recalc] datos que estan llegando desde el summary de capex {name}: {self.capex_summary_vectors.get(name, self.dep_capex)}')
                

        else:
            #pyg_simple_calculations_B.extend(['Depreciación del Periodo', 'Amortización del Periodo'])
            self.current_pyg_totals['EBIT']['dependencies'].pop(-1)
            self.current_pyg_totals['EBIT']['is_sum'].pop(-1)
            for name in ['Depreciación del Periodo', 'Amortización del Periodo']:
                self.pyg_values_vectors[name][self.historic_dates_len:] = [0] * self.projection_dates_len


        if self.coupled_debt_records:
            interest_proy_vector = [item['INTEREST_VALUE'] for item in self.coupled_debt_records]
            self.pyg_values_vectors['Intereses/gasto financiero'][-self.projection_dates_len:] = interest_proy_vector
            self.pyg_hints_vectors['Intereses/gasto financiero'][-self.projection_dates_len:] = ['Modificado por ventana modal de deuda'] * self.projection_dates_len
        


    @handler_wrapper('Creando records de pyg para carga en bd', 'Records de pyg creados con exito', 'Error creando records de pyg', 'Error creando tablas de pyg en bd')
    def final_pyg(self, save_to_db = True):
        logger.info(f'[mira aca] antes de nuevo calculo de totales: {self.pyg_values_vectors}')
        for total_partial, total_dict in self.current_pyg_totals.items():
            self.calculate_partial_totals(total_partial, total_dict, self.all_dates_len)

        logger.info(f'[mira aca] despues de nuevo calculo de totales: {self.pyg_values_vectors}')

        if save_to_db:
            self.create_pyg_records()
            self.df_and_upload(self.pyg_results_records, 'PYG_RESULTS')
            
        #logger.info(f'[mira pyg aca] pyg que le mando a flujo de caja:\n{self.pyg_values_vectors}')


        
    @handler_wrapper('Creando records de pyg para carga en bd', 'Records de pyg creados con exito', 'Error creando records de pyg', 'Error creando tablas de pyg en bd')
    def create_pyg_records(self):
        self.pyg_results_records = []
        for name, values_vector in self.pyg_values_vectors.items():
            try:
                records_to_add = [asdict(pyg_results_object(self.easy_raw_pyg_dict[name], *items)) for items in zip(self.all_dates_long, values_vector, self.pyg_hints_vectors[name])]
            except:
                logger.info(f'[create_pyg_records] No se encontraron propiedades para {name}, lo cual puede ser raro o directamente estar mal')
                continue
            if len(records_to_add) != self.all_dates_len:
                raise Exception (f'revisar código: {name} se quedó corto:\nvalues:{values_vector}\nHint: {self.pyg_hints_vectors[name]}\nDates: {self.all_dates_long}')
            self.pyg_results_records.extend( records_to_add )
