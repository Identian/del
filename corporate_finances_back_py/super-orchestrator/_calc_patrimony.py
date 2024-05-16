from decorators import handler_wrapper, timing, debugger_wrapper, runs_ok
import logging
import pandas as pd
import datetime
import sys
from _projection_methods import projection_calc_class
from dataclasses_box import patrimony_results_object, modal_windows_projected_object, asdict

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class patrimony_class(projection_calc_class):
    @handler_wrapper('Actualizando pantalla de patrimonio', 'Patrimonio actualizado con exito', 'Error actualizando patrimonio', 'Error actualizando patrimonio')
    def patrimony_recalc(self):
        self.patrimony_info = [item for item in self.assessment_models['MODEL_MODAL_WINDOWS'] if item['CONTEXT_WINDOW'] == 'patrimony']
        if not self.patrimony_info:
            logger.warning('[patrimony_class] No se encontró modelo de patrimonio')
            #self.noting_list.append({'TASK': self.current_context, 'NOTE': "No se halló modelo para calculo de patrimonio"})
            return
            
        self.organizer = {
                "Cero": self.patrimony_zero,
                "Valor constante": self.patrimony_constant,
                "Input": self.patrimony_input,
                "Tasa de crecimiento fija": self.patrimony_fixed,
                "Porcentaje de otra variable": self.patrimony_master_dependencies,
                "Tasas impositivas": self.patrimony_master_dependencies}
    
        self.patrimony_projected_vectors = dict()
        self.patrimony_account_history_vector = dict()
        self.patrimony_classification_easy_dict = dict()
        self.pat_social_capital_contributions = list()
        self.patrimony_proy_records = list()
        
        self.pat_add_merge_account_original_values()
        self.patrimony_projections_loop()
        self.get_patrimony_historical_values()
        
        self.patrimony_organize_items()
        self.patrimony_summary()
        
        self.df_and_upload(self.patrimony_info, 'MODAL_WINDOWS', and_clause='AND CONTEXT_WINDOW = "patrimony"')

        self.df_and_upload(self.patrimony_proy_records, 'MODAL_WINDOWS_PROJECTED', and_clause='AND CONTEXT_WINDOW = "patrimony"')
        
        self.df_and_upload(self.patrimony_results_records, 'PATRIMONY_RESULTS')
        
    
    @handler_wrapper('Emergiendo valores originales de las cuentas proyectadas', 'Valores originales adquiridos', 'Error emergiendo valores originales de cuentas proyectadas en patrmonio', 'Error adquiriendo valores originales de cuentas de patrimonio')
    def pat_add_merge_account_original_values(self):
        assessment_accounts = [item['account'] for item in self.purged_items if item['classification'] in ['Aportes de capital social u otros', 'Cambios en el patrimonio']]
        modal_window_model_accounts = [item['ACCOUNT_NUMBER'] for item in self.patrimony_info]

        for account in assessment_accounts:
            if account not in modal_window_model_accounts:
                self.patrimony_info.append({'ACCOUNT_NUMBER': account, 'CONTEXT_WINDOW':'patrimony', 'VS_ACCOUNT_NAME': 'Seleccione', 'PROJECTION_TYPE': 'Cero', 'COMMENT': 'Cuenta agregada por actualización de clasificaciones'})
                self.noting_list.append({'TASK': self.current_context, 'NOTE': f'Se agrega la cuenta {account} a la ventana modal de patrimonio'})
                
        for account in modal_window_model_accounts:
            if account not in assessment_accounts:
                self.patrimony_info = [item for item in self.patrimony_info if item['ACCOUNT_NUMBER'] != account]
                self.noting_list.append({'TASK': self.current_context, 'NOTE': f'Se elimina la cuenta {account} de la ventana modal de patrimonio'})
        
        for item in self.patrimony_info:
            item['ORIGINAL_VALUE'] = self.get_historic_account_values(item['ACCOUNT_NUMBER'])[-1]
        
    
        
    @handler_wrapper('Iniciando loop de proyecciones de patrimonio', 'Loop de patrimonio terminado', 'Error calculando loop de patrimonio', 'Error calculando patrimonio')    
    def patrimony_projections_loop(self):
        items_to_project = self.patrimony_info.copy()
        max_loops = len(items_to_project) * len(items_to_project)
        while max_loops:
            if not items_to_project:
                logger.warning('[patrimony_projections_loop] Ya se acabaron los datos para proyectar')
                break
            
            projecting_item = items_to_project.pop(0)
            projected_success = self.organizer[projecting_item['PROJECTION_TYPE']](projecting_item)
            if not projected_success:
                logger.warning(f'[patrimony_projections_loop] el item {projecting_item} no fue proyectado')
                items_to_project.append(projecting_item)
            max_loops = max_loops-1
        
        if not max_loops:
            logger.warning('[patrimony_projections_loop] El loop se fue a infinito, pero no deberia porque depende de pyg que ya debería estar totalmente calculado')
            infinite_cycle_rows = [row['ACCOUNT_NUMBER'] for row in items_to_project]
            infinite_cycle_rows_str = str(infinite_cycle_rows).replace('[', '').replace(']', '')
            self.noting_list.append({'TASK': self.current_context, 'NOTE': f"Las sigtes filas de pyg se fueron a ciclo infinito {infinite_cycle_rows_str}"})
            

    @runs_ok
    @debugger_wrapper('Error proyectando zeros', 'Error proyectando un item con proyeccion cero')
    def patrimony_zero(self, projecting_item):
        self.patrimony_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = self.pm_calculate_zero()


    @runs_ok
    @debugger_wrapper('Error proyectando constante', 'Error proyectando un item con proyeccion constante')
    def patrimony_constant(self, projecting_item):
        self.patrimony_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = self.pm_calculate_constant(projecting_item['ORIGINAL_VALUE'])

    
    @runs_ok
    @debugger_wrapper('Error proyectando input', 'Error proyectando un item con proyeccion input')
    def patrimony_input(self, projecting_item):
        projections = [float(item['ATRIBUTE']) for item in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] if item['CONTEXT_WINDOW'] == 'patrimony' and item['ACCOUNT_NUMBER'] == projecting_item['ACCOUNT_NUMBER']]
        self.patrimony_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = projections

    
    @runs_ok
    @debugger_wrapper('Error proyectando porcentaje fijo', 'Error proyectando un item con proyeccion a porcentajes')
    def patrimony_fixed(self, projecting_item):
        fixed_percentages = [float(item['ATRIBUTE']) for item in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] if item['CONTEXT_WINDOW'] == 'patrimony' and item['ACCOUNT_NUMBER'] == projecting_item['ACCOUNT_NUMBER']]
        self.patrimony_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = self.pm_calculate_fixed(projecting_item['ORIGINAL_VALUE'], fixed_percentages)

    
    @runs_ok
    @debugger_wrapper("Error en calculo de proyecciones proporcionales", "Error calculando proyecciones") #si le colocas el debugger_wrapper se sobreescribe el AttributeError('personalizada') que espera master_items_loop
    def patrimony_master_dependencies(self, projecting_item):
        vs_vector = self.pyg_values_vectors.get(projecting_item['VS_ACCOUNT_NAME'])
        if projecting_item["PROJECTION_TYPE"] == 'Porcentaje de otra variable':
            items_projections = self.pm_calculate_proportion(projecting_item['ORIGINAL_VALUE'], vs_vector)
        if projecting_item["PROJECTION_TYPE"] == 'Tasas impositivas':
            #logger.debug(f'[patrimony_master_dependencies] A esto esta dando error: {projecting_item}')
            atributes = [float(item['ATRIBUTE']) for item in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] if item['CONTEXT_WINDOW'] == 'patrimony' and item['ACCOUNT_NUMBER'] == projecting_item['ACCOUNT_NUMBER']]
            #logger.debug(f'[patrimony_master_dependencies] B esto esta dando error: {atributes}')
            items_projections = self.pm_calculate_impositive(atributes, vs_vector)
            #logger.debug(f'[patrimony_master_dependencies] C esto esta dando error: {items_projections}')
        self.patrimony_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = items_projections 


    

    @handler_wrapper('Organizando items basicos de patrimonio', 'Resultados basicos de patrimonio calculados con exito', 'Error calculando bases de datos de patrimonio', 'Error organizando tablas de patrimonio')
    def patrimony_organize_items(self):
        for item in self.patrimony_info:
            account = item['ACCOUNT_NUMBER']
            atri_vect = [row['ATRIBUTE'] for row in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] if (row['CONTEXT_WINDOW'] == 'patrimony' and row['ACCOUNT_NUMBER'] == account)]
            if not atri_vect:
                atri_vect = [''] * len(self.projection_dates_long)
            logger.info(f'[mira aca] algo de esto está botando problemas: A {self.patrimony_projected_vectors[account]} \nB:{self.projection_dates_long}\nC:{atri_vect}')
            self.patrimony_proy_records.extend([asdict(modal_windows_projected_object(proy_value, account, date, atri, 'patrimony')) for proy_value, date, atri in zip(self.patrimony_projected_vectors[account], self.projection_dates_long, atri_vect)])
    
    
    @handler_wrapper('Creando directorio easy de cuentas y vector de historicos', 'Directorio de historicos creado con exito', 'Error creando directorio de historicos', 'Error organizando vector de valores historicos')
    def get_patrimony_historical_values(self):
        self.patrimony_account_history_vector = {account: self.get_historic_account_values(account) for account in self.patrimony_projected_vectors}
        for classification in ['Aportes de capital social u otros', 'Cambios en el patrimonio']:
            self.patrimony_classification_easy_dict[classification] = set(item['account'] for item in self.purged_items if item['account'] in self.patrimony_projected_vectors and item['classification'] == classification)
            
    
    @handler_wrapper('Construyendo summary de patrimonio', 'Lógica de patrimonio construída con exito', 'Error construyendo lógica de negocio de patrimonio', 'Error calculando resultados de patrimonio')
    def patrimony_summary(self):
        full_patrimony_vectors = {account: self.patrimony_account_history_vector[account] + self.patrimony_projected_vectors[account] for account in self.patrimony_projected_vectors}
        classification_vector = {}
        for classification in ['Aportes de capital social u otros', 'Cambios en el patrimonio']:
            accounts_vectors = [full_patrimony_vectors[account] for account in self.patrimony_classification_easy_dict[classification]]
            classification_vector[classification] = [sum(items) for items in zip(*accounts_vectors)]

        social_capital_contributions_deltas = [0]
        for index, value in enumerate(classification_vector['Aportes de capital social u otros'][1:], start = 1):
            social_capital_contributions_deltas.append(value - classification_vector['Aportes de capital social u otros'][index-1])
        
        classification_vector['Cambios en el patrimonio'] = classification_vector['Cambios en el patrimonio'] if classification_vector['Cambios en el patrimonio'] else [0] * self.all_dates_len
        cash_dividends = [-classification_vector['Cambios en el patrimonio'][0]]
        for index, value in enumerate(classification_vector['Cambios en el patrimonio'][1:], start = 1):
            cash_dividends.append(self.pyg_values_vectors['Utilidad neta'][index-1] - value + classification_vector['Cambios en el patrimonio'][index-1])
            
        self.patrimony_results_records = [asdict(patrimony_results_object(*items)) for items in zip(self.all_dates_long, social_capital_contributions_deltas, cash_dividends)]
        
