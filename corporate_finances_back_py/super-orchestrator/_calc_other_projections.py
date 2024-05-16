from decorators import handler_wrapper, timing, debugger_wrapper, runs_ok
import logging
import pandas as pd
import datetime
import sys
from _projection_methods import projection_calc_class
from dataclasses_box import other_modal_results_object, modal_windows_projected_object, asdict

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class other_projections_class(projection_calc_class):
    @handler_wrapper('Actualizando pantalla de capital de trabajo', 'Capital de trabajo actualizado con exito', 'Error actualizando capital de trabajo', 'Error actualizando capital de trabajo')
    def other_projections_recalc(self):
        self.op_info = [item for item in self.assessment_models['MODEL_MODAL_WINDOWS'] if item['CONTEXT_WINDOW'] == 'other_projections']
        if not self.op_info:
            logger.warning('[working_capital_class] No se encontró modelo de capital de trabajo')
            #self.noting_list.append({'TASK': self.current_context, 'NOTE': "No se halló modelo para calculo de otras proyecciones"})
            return
            
        self.organizer = {
                "Cero": self.op_zero,
                "Valor constante": self.op_constant,
                "Input": self.op_input,
                "Tasa de crecimiento fija": self.op_fixed,
                "Porcentaje de otra variable": self.op_master_dependencies,
                "Tasas impositivas": self.op_master_dependencies}
    
        self.op_projected_vectors = dict()
        self.op_proy_records = list()
        
        self.op_add_merge_account_original_values()
        self.op_projections_loop()
        
        self.op_organize_items()
        self.calculate_op_inputs()
        self.op_summary()
        
        self.df_and_upload(self.op_info, 'MODAL_WINDOWS', and_clause='AND CONTEXT_WINDOW = "other_projections"')

        self.df_and_upload(self.op_proy_records, 'MODAL_WINDOWS_PROJECTED', and_clause='AND CONTEXT_WINDOW = "other_projections"')
        
        self.df_and_upload(self.op_results_records, 'OTHER_MODAL_RESULTS')
        
    
    @handler_wrapper('Emergiendo valores originales de las cuentas proyectadas', 'Valores originales adquiridos', 'Error emergiendo valores originales de cuentas proyectadas en patrmonio', 'Error adquiriendo valores originales de cuentas de patrimonio')
    def op_add_merge_account_original_values(self):
        assessment_accounts = [item['account'] for item in self.purged_items if item['classification'] in ['Otros movimientos que no son salida ni entrada de efectivo no operativos','Otros movimientos que no son salida ni entrada de efectivo operativos','Otros movimientos netos de activos operativos que afecta el FCLO','Otros movimientos netos de activos operativos que afecta el FCLA']]
        modal_window_model_accounts = [item['ACCOUNT_NUMBER'] for item in self.op_info]

        for account in assessment_accounts:
            if account not in modal_window_model_accounts:
                self.op_info.append({'ACCOUNT_NUMBER': account, 'CONTEXT_WINDOW':'other_projections', 'VS_ACCOUNT_NAME': 'Seleccione', 'PROJECTION_TYPE': 'Cero', 'COMMENT': 'Cuenta agregada por actualización de clasificaciones'})
                self.noting_list.append({'TASK': self.current_context, 'NOTE': f'Se agrega la cuenta {account} a la ventana modal de otras proyecciones'})
                
        for account in modal_window_model_accounts:
            if account not in assessment_accounts:
                self.op_info = [item for item in self.op_info if item['ACCOUNT_NUMBER'] != account]
                self.noting_list.append({'TASK': self.current_context, 'NOTE': f'Se elimina la cuenta {account} de la ventana modal de otras proyecciones'})
                
        for item in self.op_info:
            item['ORIGINAL_VALUE'] = self.get_historic_account_values(item['ACCOUNT_NUMBER'])[-1]
        
    
        
    @handler_wrapper('Iniciando loop de proyecciones de capital de trabajo', 'Loop de capital de trabajo terminado', 'Error calculando loop de capital de trabajo', 'Error calculando capital de trabajo')    
    def op_projections_loop(self):
        items_to_project = self.op_info.copy()
        max_loops = len(items_to_project) * len(items_to_project)
        while max_loops:
            if not items_to_project:
                logger.warning(f'[op_projections_loop] Ya se acabaron los datos para proyectar')
                break
            
            projecting_item = items_to_project.pop(0)
            projected_success = self.organizer[projecting_item['PROJECTION_TYPE']](projecting_item)
            if not projected_success:
                logger.warning(f'[op_projections_loop] el item {projecting_item} no fue proyectado')
                items_to_project.append(projecting_item)
            max_loops = max_loops-1
        
        if not max_loops:
            logger.warning(f'[op_projections_loop] El loop se fue a infinito, pero no deberia porque depende de pyg que ya debería estar totalmente calculado')
            infinite_cycle_rows = [row['ACCOUNT_NUMBER'] for row in items_to_project]
            infinite_cycle_rows_str = str(infinite_cycle_rows).replace('[', '').replace(']', '')
            self.noting_list.append({'TASK': self.current_context, 'NOTE': f"Las sigtes filas de pyg se fueron a ciclo infinito {infinite_cycle_rows_str}"})
            

    @runs_ok
    @debugger_wrapper('Error proyectando zeros', 'Error proyectando un item con proyeccion cero')
    def op_zero(self, projecting_item):
        self.op_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = self.pm_calculate_zero()


    @runs_ok
    @debugger_wrapper('Error proyectando constante', 'Error proyectando un item con proyeccion constante')
    def op_constant(self, projecting_item):
        self.op_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = self.pm_calculate_constant(projecting_item['ORIGINAL_VALUE'])

    
    @runs_ok
    @debugger_wrapper('Error proyectando input', 'Error proyectando un item con proyeccion input')
    def op_input(self, projecting_item):
        projections = [float(item['ATRIBUTE']) for item in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] if item['CONTEXT_WINDOW'] == 'other_projections' and item['ACCOUNT_NUMBER'] == projecting_item['ACCOUNT_NUMBER']]
        self.op_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = projections

    
    @runs_ok
    @debugger_wrapper('Error proyectando porcentaje fijo', 'Error proyectando un item con proyeccion a porcentajes')
    def op_fixed(self, projecting_item):
        fixed_percentages = [float(item['ATRIBUTE']) for item in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] if item['CONTEXT_WINDOW'] == 'other_projections' and item['ACCOUNT_NUMBER'] == projecting_item['ACCOUNT_NUMBER']]
        self.op_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = self.pm_calculate_fixed(projecting_item['ORIGINAL_VALUE'], fixed_percentages)

    
    @runs_ok
    @debugger_wrapper("Error en calculo de proyecciones proporcionales", "Error calculando proyecciones") #si le colocas el debugger_wrapper se sobreescribe el AttributeError('personalizada') que espera master_items_loop
    def op_master_dependencies(self, projecting_item):
        vs_vector = self.pyg_values_vectors.get(projecting_item['VS_ACCOUNT_NAME'])
        if projecting_item["PROJECTION_TYPE"] == 'Porcentaje de otra variable':
            items_projections = self.pm_calculate_proportion(projecting_item['ORIGINAL_VALUE'], vs_vector)
        if projecting_item["PROJECTION_TYPE"] == 'Tasas impositivas':
            atributes = [float(item['ATRIBUTE']) for item in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] if item['CONTEXT_WINDOW'] == 'other_projections' and item['ACCOUNT_NUMBER'] == projecting_item['ACCOUNT_NUMBER']]
            items_projections = self.pm_calculate_impositive(atributes, vs_vector)
        self.op_projected_vectors[projecting_item['ACCOUNT_NUMBER']] = items_projections 


    @handler_wrapper('Organizando items basicos de capital de trabajo', 'Resultados basicos de capital de trabajo calculados con exito', 'Error calculando bases de datos de capital de trabajo', 'Error organizando tablas de capital de trabajo')
    def op_organize_items(self):
        for item in self.op_info:
            account = item['ACCOUNT_NUMBER']
            atri_vect = [row['ATRIBUTE'] for row in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] if (row['CONTEXT_WINDOW'] == 'other_projections' and row['ACCOUNT_NUMBER'] == account)]
            if not atri_vect:
                atri_vect = [''] * len(self.projection_dates_long)
            self.op_proy_records.extend([asdict(modal_windows_projected_object(proy_value, account, date, atri, 'other_projections')) for proy_value, date, atri in zip(self.op_projected_vectors[account], self.projection_dates_long, atri_vect)])


    @handler_wrapper('Creando directorio easy de cuentas y vector de historicos', 'Directorio de historicos creado con exito', 'Error creando directorio de historicos', 'Error organizando vector de valores historicos')
    def calculate_op_inputs(self):
        accounts_full_vectors = {}
        for account, proy_vector in self.op_projected_vectors.items():
            historic_vector = self.get_historic_account_values(account)
            accounts_full_vectors[account] = historic_vector + proy_vector
            
        self.variations_vectors = {}
        for classification in ['Otros movimientos que no son salida ni entrada de efectivo operativos', 'Otros movimientos netos de activos operativos que afecta el FCLO', 'Otros movimientos netos de activos operativos que afecta el FCLA']:
            classification_accounts = set(item['account'] for item in self.purged_items if item['account'] in self.op_projected_vectors and item['classification'] == classification)
            if classification_accounts:
                accounts_vectors = [accounts_full_vectors[account] for account in classification_accounts]
                classification_vector = [sum(items) for items in zip(*accounts_vectors)]        
            else:
                classification_vector = [0] * self.all_dates_len
            self.variations_vectors[classification] = [0] + [ value - classification_vector[previous_index] for previous_index, value in enumerate(classification_vector[1:])]
        logger.warning(f'[calculate_op_inputs] Resultados de variaciones para otras proyecciones:\n{self.variations_vectors}')

        
    @handler_wrapper('Construyendo summary de Otras proyecciones', 'Lógica de otras proyecciones construída con exito', 'Error construyendo lógica de negocio de otras proyecciones', 'Error calculando resultados de otras proyecciones')
    def op_summary(self):
        self.op_results_records = [asdict(other_modal_results_object(date, i,j,k)) for date, i, j, k in zip(self.all_dates_long, *list(self.variations_vectors.values()))]
        
        
def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)