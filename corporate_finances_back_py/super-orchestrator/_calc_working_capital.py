from decorators import handler_wrapper, timing, debugger_wrapper, runs_ok
import logging
import pandas as pd
import datetime
import sys
from _projection_methods import projection_calc_class
from dataclasses_box import wk_results_object, modal_windows_projected_object, asdict

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class working_capital_class(projection_calc_class):
    @handler_wrapper('Actualizando pantalla de capital de trabajo', 'Capital de trabajo actualizado con exito', 'Error actualizando capital de trabajo', 'Error actualizando capital de trabajo')
    def working_capital_recalc(self):
        self.wk_info = [item for item in self.assessment_models['MODEL_MODAL_WINDOWS'] if item['CONTEXT_WINDOW'] == 'wk']
        if not self.wk_info:
            logger.warning(f'[working_capital_class] No se encontró modelo de capital de trabajo')
            #self.noting_list.append({'TASK': self.current_context, 'NOTE': f"No se halló modelo para calculo de capital de trabajo"})
            return
            
        self.organizer = {
                "Cero": self.wk_zero,
                "Valor constante": self.wk_constant,
                "Input": self.wk_input,
                "Tasa de crecimiento fija": self.wk_fixed,
                "Porcentaje de otra variable": self.wk_master_dependencies,
                "Tasas impositivas": self.wk_master_dependencies}
    
        self.wk_projected_vectors = dict()
        self.wk_db_records = list()
        
        self.wk_add_merge_account_original_values()
        self.wk_projections_loop()
        
        self.wk_organize_items()
        self.calculate_wk_inputs()
        self.wk_summary()
        
        self.df_and_upload(self.wk_info, 'MODAL_WINDOWS', and_clause='AND CONTEXT_WINDOW = "wk"')

        self.df_and_upload(self.wk_db_records, 'MODAL_WINDOWS_PROJECTED', and_clause='AND CONTEXT_WINDOW = "wk"')
        
        self.df_and_upload(self.wk_results_records, 'WK_RESULTS')
        
    
    @handler_wrapper('Emergiendo valores originales de las cuentas proyectadas', 'Valores originales adquiridos', 'Error emergiendo valores originales de cuentas proyectadas en patrmonio', 'Error adquiriendo valores originales de cuentas de patrimonio')
    def wk_add_merge_account_original_values(self):
        assessment_accounts = [item['account'] for item in self.purged_items if item['classification'] == 'Capital de trabajo']
        modal_window_model_accounts = [item['ACCOUNT_NUMBER'] for item in self.wk_info]

        for account in assessment_accounts:
            if account not in modal_window_model_accounts:
                self.wk_info.append({'ACCOUNT_NUMBER': account, 'CONTEXT_WINDOW':'wk', 'VS_ACCOUNT_NAME': 'Seleccione', 'PROJECTION_TYPE': 'Cero', 'COMMENT': 'Cuenta agregada por actualización de clasificaciones'})
                self.noting_list.append({'TASK': self.current_context, 'NOTE': f'Se agrega la cuenta {account} a la ventana modal de capital de trabajo'})
                
        for account in modal_window_model_accounts:
            if account not in assessment_accounts:
                self.wk_info = [item for item in self.wk_info if item['ACCOUNT_NUMBER'] != account]
                self.noting_list.append({'TASK': self.current_context, 'NOTE': f'Se elimina la cuenta {account} de la ventana modal de capital de trabajo'})
        
        for item in self.wk_info:
            item['ORIGINAL_VALUE'] = self.get_historic_account_values(item['ACCOUNT_NUMBER'])[-1]
        
    

    @handler_wrapper('Iniciando loop de proyecciones de capital de trabajo', 'Loop de capital de trabajo terminado', 'Error calculando loop de capital de trabajo', 'Error calculando capital de trabajo')    
    def wk_projections_loop(self):
        items_to_project = self.wk_info.copy()
        logger.info(f'[mira aca] esto es lo que se va a loopear de capital de trabajo: {items_to_project}')
        max_loops = len(items_to_project) * len(items_to_project)
        while max_loops:
            if not items_to_project:
                logger.warning(f'[wk_projections_loop] Ya se acabaron los datos para proyectar')
                break
            
            projecting_item = items_to_project.pop(0)
            projected_success = self.organizer[projecting_item['PROJECTION_TYPE']](projecting_item)
            if not projected_success:
                logger.warning(f'[wk_projections_loop] el item {projecting_item} no fue proyectado')
                items_to_project.append(projecting_item)
            max_loops = max_loops-1
        
        if not max_loops:
            logger.warning(f'[wk_projections_loop] El loop se fue a infinito, pero no deberia porque depende de pyg que ya debería estar totalmente calculado')
            infinite_cycle_rows = [row['ACCOUNT_NUMBER'] for row in items_to_project]
            infinite_cycle_rows_str = str(infinite_cycle_rows).replace('[', '').replace(']', '')
            self.noting_list.append({'TASK': self.current_context, 'NOTE': f"Las sigtes filas de pyg se fueron a ciclo infinito {infinite_cycle_rows_str}"})
        
        logger.info(f'[mira aca] resultado de los loops de capital de trabajo {self.wk_projected_vectors} ')
            

    @runs_ok
    @debugger_wrapper('Error proyectando zeros', 'Error proyectando un item con proyeccion cero')
    def wk_zero(self, projecting_item):
        self.wk_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = self.pm_calculate_zero()


    @runs_ok
    @debugger_wrapper('Error proyectando constante', 'Error proyectando un item con proyeccion constante')
    def wk_constant(self, projecting_item):
        self.wk_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = self.pm_calculate_constant(projecting_item['ORIGINAL_VALUE'])

    
    @runs_ok
    @debugger_wrapper('Error proyectando input', 'Error proyectando un item con proyeccion input')
    def wk_input(self, projecting_item):
        projections = [float(item['ATRIBUTE']) for item in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] if item['CONTEXT_WINDOW'] == 'wk' and item['ACCOUNT_NUMBER'] == projecting_item['ACCOUNT_NUMBER']]
        self.wk_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = projections

    
    @runs_ok
    @debugger_wrapper('Error proyectando porcentaje fijo', 'Error proyectando un item con proyeccion a porcentajes')
    def wk_fixed(self, projecting_item):
        fixed_percentages = [float(item['ATRIBUTE']) for item in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] if item['CONTEXT_WINDOW'] == 'wk' and item['ACCOUNT_NUMBER'] == projecting_item['ACCOUNT_NUMBER']]
        self.wk_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = self.pm_calculate_fixed(projecting_item['ORIGINAL_VALUE'], fixed_percentages)

    
    @runs_ok
    @debugger_wrapper("Error en calculo de proyecciones proporcionales", "Error calculando proyecciones") #si le colocas el debugger_wrapper se sobreescribe el AttributeError('personalizada') que espera master_items_loop
    def wk_master_dependencies(self, projecting_item):
        vs_vector = self.pyg_values_vectors.get(projecting_item['VS_ACCOUNT_NAME'])
        if projecting_item["PROJECTION_TYPE"] == 'Porcentaje de otra variable':
            logger.info(f"[mira aca] le voy a inyectar: {projecting_item['ORIGINAL_VALUE']} y vs vector {vs_vector}")
            items_projections = self.pm_calculate_proportion(projecting_item['ORIGINAL_VALUE'], vs_vector)
            logger.info(f"[mira aca] con resultados: {items_projections}")
        if projecting_item["PROJECTION_TYPE"] == 'Tasas impositivas':
            atributes = [float(item['ATRIBUTE']) for item in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] if item['CONTEXT_WINDOW'] == 'wk' and item['ACCOUNT_NUMBER'] == projecting_item['ACCOUNT_NUMBER']]
            items_projections = self.pm_calculate_impositive(atributes, vs_vector)
        self.wk_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = items_projections 


    @handler_wrapper('Organizando items basicos de capital de trabajo', 'Resultados basicos de capital de trabajo calculados con exito', 'Error calculando bases de datos de capital de trabajo', 'Error organizando tablas de capital de trabajo')
    def wk_organize_items(self):
        for item in self.wk_info:
            account = item['ACCOUNT_NUMBER']
            atri_vect = [row['ATRIBUTE'] for row in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] if (row['CONTEXT_WINDOW'] == 'wk' and row['ACCOUNT_NUMBER'] == account)]
            if not atri_vect:
                atri_vect = [''] * len(self.projection_dates_long)
            self.wk_db_records.extend([asdict(modal_windows_projected_object(proy_value, account, date, atri, 'wk')) for proy_value, date, atri in zip(self.wk_projected_vectors[account], self.projection_dates_long, atri_vect)])


    @handler_wrapper('Creando directorio easy de cuentas y vector de historicos', 'Directorio de historicos creado con exito', 'Error creando directorio de historicos', 'Error organizando vector de valores historicos')
    def calculate_wk_inputs(self):
        self.active_pasive_vectors = {}
        for account, proy_vector in self.wk_projected_vectors.items():
            historic_values = self.get_historic_account_values(account)
            summed_vector = [i+j for i,j in zip(self.active_pasive_vectors.get(account[0], [0] * self.all_dates_len), historic_values + proy_vector)]
            self.active_pasive_vectors[account[0]] = summed_vector
        logger.info(f'[calculate_wk_inputs] Vectorización de capital de trabajo:\n{self.active_pasive_vectors}')
        
    @handler_wrapper('Construyendo summary de capital de trabajo', 'Lógica de capital de trabajo construída con exito', 'Error construyendo lógica de negocio de capital de trabajo', 'Error calculando resultados de capital de trabajo')
    def wk_summary(self):
        self.yearly_working_capital = [i-j for i,j in zip(self.active_pasive_vectors['1'], self.active_pasive_vectors['2'])]
        self.wk_results = [value - self.yearly_working_capital[previous_index] for previous_index, value in enumerate(self.yearly_working_capital[1:])]
        self.wk_results.insert(0, 0)
        logger.info(f'[wk_summary] Resultado summary de working capital:\n{self.wk_results}')
        self.wk_results_records = [asdict(wk_results_object(date, variation)) for date, variation in zip(self.all_dates_long, self.wk_results)]
        
        
        
        
def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)