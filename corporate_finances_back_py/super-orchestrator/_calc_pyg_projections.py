from decorators import handler_wrapper, timing, debugger_wrapper
from dataclasses_box import pyg_item_object, pyg_projected_object, asdict
import logging
import pandas as pd
import datetime
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class pyg_projections_class(object):
    @handler_wrapper('Actualizando proyecciones de pyg', 'Proyecciones de pyg calculadas', 'Error calculando proyecciones de pyg', 'Error actualizando proyecciones pyg')
    def pyg_first_half_projections_recalc(self): 
        if not self.assessment_models.get('MODEL_PROJECTED_PYG', False): #TODO: si acá en el modelo le saco las proyecciones sum, puedo ejecutar todo el loop sin necesidad de usar el metodo 'calculate_sum'
            logger.warning(f'[pyg_projections_recalc] No hay atributos para las proyecciones de pyg')
            #self.noting_list.append({'TASK': self.current_context, 'NOTE': f"No se halló información de proyeccion de pyg"})
            self.assessment_projections_found = False
            return
        self.obligatory_pyg_items = ['Costos (Sin depreciación)',
                            #'Depreciación del Periodo',
                            #'Amortización del Periodo',
                            'Otros ingresos y egresos operativos',
                            'Otros ingresos y egresos no operativos',
                            'Deterioro',
                            'Impuestos de renta',
                            'Intereses/gasto financiero',
                            'Ingresos operacionales',
                            'Gastos operacionales',
                            'Utilidad bruta',
                            'EBITDA',
                            'EBIT',
                            'Utilidad antes de impuestos',
                            'Utilidad neta',
                            'Depreciación Capex']
        
        self.organizer = {
                "Cero": self.calculate_zero,
                "Valor constante": self.calculate_constant,
                "Input": self.calculate_input,
                "Tasa de crecimiento fija": self.calculate_fixed,
                "Porcentaje de otra variable": self.master_calculate_dependencies,
                "Tasas impositivas": self.master_calculate_dependencies,
                "sum": self.master_calculate_dependencies}
        
        self.pyg_projected_vectors = {}
        self.pyg_projected_hints = {}
        
        self.initialize_zero_vectors()
        self.merge_pyg_raw()
        self.merge_summaries()
        self.merge_atributes()
        self.pyg_projections_master_loop()
        logger.info(f'[mira aca] Al terminar el loop tengo estas proyecciones: {self.pyg_projected_vectors}')
        self.calculate_projections_totals()
        logger.warning(f'[pyg_projections_recalc] Vectores resultados de proyecciones:\n{self.pyg_projected_vectors}')
        self.operation_income_vector = self.pyg_values_vectors['Ingresos operacionales'] + self.pyg_projected_vectors['Ingresos operacionales']
        self.save_assessment_step('PYG')
   
    @handler_wrapper('Inicializando vectores en cero para las proyecciones de pyg', 'Vectores inicializados en cero', 'Error inicializando vectores de proyecciones de pyg en cero', 'Error inicializando vectores')
    def initialize_zero_vectors(self):
        for pyg_row in self.pyg_values_vectors:
            self.pyg_projected_vectors[pyg_row] = [0] * self.projection_dates_len
        pyg_item_model_names = [self.id_raw_pyg_dict[item['ID_RAW_PYG']] for item in self.assessment_models['MODEL_PYG_ITEM']]
        for item in self.obligatory_pyg_items:
            if item not in pyg_item_model_names:
                self.assessment_models['MODEL_PYG_ITEM'].append({'ID_RAW_PYG': self.easy_raw_pyg_dict[item], 'PROJECTION_TYPE': 'Cero', 'COMMENT': 'Cuenta obligatoria creada ya que fue omitida en el modelo', 'ID_DEPENDENCE': self.easy_raw_pyg_dict['No aplica']})


    @handler_wrapper('Emergiendo raw de pyg con datos existentes', 'Raw pyg emergido con exito', 'Error emergiendo raw de pyg', 'Error asignando nombres de pyg')
    def merge_pyg_raw(self):
        logger.warning(f'[mira aca] este es el modelo original: {self.assessment_models["MODEL_PYG_ITEM"]}')
        for item in self.assessment_models['MODEL_PYG_ITEM']:
            item['classification'] = self.id_raw_pyg_dict[item['ID_RAW_PYG']]
            item['dependence'] = self.id_raw_pyg_dict[item['ID_DEPENDENCE']]
            self.pyg_projected_hints[item['classification']] = item['PROJECTION_TYPE']
            if item['classification'].startswith(('Ingresos operacionales ','Gastos operacionales ')) and item['classification'] not in self.pyg_values_vectors:
                self.noting_list.append({'TASK': self.current_context, 'NOTE': f'La cuenta {item["classification"]} proyectada, dejó de existir en el registro de clasificaciones'})

        self.assessment_models['MODEL_PYG_ITEM'] = [item for item in self.assessment_models['MODEL_PYG_ITEM'] if item['classification'] in self.pyg_values_vectors]
        
        for item_name in [ item for item in self.pyg_values_vectors if item.startswith(('Ingresos operacionales ','Gastos operacionales '))]:
            if not next((projection_item for projection_item in self.assessment_models['MODEL_PYG_ITEM'] if projection_item['classification'] == item_name), False):
                self.assessment_models['MODEL_PYG_ITEM'].append({'classification': item_name, 'PROJECTION_TYPE': 'Cero', 'COMMENT': 'Nueva clasificacion, proyeccion no especificada', 'ID_RAW_PYG': self.easy_raw_pyg_dict[item_name], 'ID_DEPENDENCE': self.easy_raw_pyg_dict['No aplica']})
                self.pyg_projected_hints[item_name] = 'Cero'
                self.noting_list.append({'TASK': self.current_context, 'NOTE': f'La cuenta {item_name} es nueva a la clasificación, se proyecta en ceros'})
                

    
    @handler_wrapper('Emergiendo valore originales de pyg', 'Valores originales de pyg adquiridos', 'Error adquiriendo valores originales de filas pyg', 'Error calculando pygs')
    def merge_summaries(self):
        for item in self.assessment_models['MODEL_PYG_ITEM']:
            if self.pyg_values_vectors.get(item['classification'], False):
                item['value'] = self.pyg_values_vectors[item['classification']][-1]
            else:            
                item['value'] = self.get_historic_summary_values(item['classification'])[-1]


    @handler_wrapper('Emergiendo los atributos de las clasificaciones', 'Atributos emergidos correctamente', 'Error emergiendo atributos de lineas pyg', 'Error emergiendo caracteristicas de pyg')
    def merge_atributes(self):
        for item in self.assessment_models['MODEL_PYG_ITEM']:
            item['atributes'] = [row['ATRIBUTE'] for row in self.assessment_models['MODEL_PROJECTED_PYG'] if row['ID_RAW_PYG'] == item['ID_RAW_PYG']]
            if item['PROJECTION_TYPE'] == "sum":
                try:
                    item['dependence'] = self.current_pyg_totals[item['classification']]['dependencies']
                    item['is_sum'] = self.current_pyg_totals[item['classification']]['is_sum']
                except Exception:
                    logger.warning(f'[merge_atributes] no se emergió el item de total {item["classification"]} puede que sea parte de una sección posterior de pyg')
    
    
    @handler_wrapper('Calculando loop de proyecciones', 'Loop de proyecciones terminado', 'Error calculando loop de proyecciones', 'Error calculando proyecciones')
    def pyg_projections_master_loop(self):
        logger.warning(f'[mira aca] este es el modelo a pasar a loop: {self.assessment_models["MODEL_PYG_ITEM"]}')
        rows_to_project = self.assessment_models['MODEL_PYG_ITEM'].copy()
        max_loops = len(rows_to_project) * len(rows_to_project)
        while max_loops:
            if not rows_to_project:
                logger.warning(f'[pyg_projections_master_loop] Ya se acabaron los datos para proyectar')
                break
            
            projecting_row = rows_to_project.pop(0)
            projected_success = self.organizer[projecting_row['PROJECTION_TYPE']](projecting_row)
            if not projected_success:
                #logger.warning(f'[pyg_projections_master_loop] el item {projecting_row} no fue proyectado')
                rows_to_project.append(projecting_row)
            max_loops = max_loops-1
        
        if not max_loops:
            logger.warning(f'[pyg_projections_master_loop] el loop calculador de proyecciones se fue a infinito')
            infinite_cycle_rows = [row['classification'] for row in rows_to_project]
            infinite_cycle_rows_str = str(infinite_cycle_rows).replace('[', '').replace(']', '')
            #self.noting_list.append({'TASK': self.current_context, 'NOTE': f"Las sigtes filas de pyg se fueron a ciclo infinito {infinite_cycle_rows_str}"})
            

    @debugger_wrapper('Error proyectando zeros', 'Error proyectando un item con proyeccion cero')
    def calculate_zero(self, projecting_item):
        projections = [0] * self.projection_dates_len
        self.pyg_projected_vectors[projecting_item['classification']] = projections
        return True


    @debugger_wrapper('Error proyectando constante', 'Error proyectando un item con proyeccion constante')
    def calculate_constant(self, projecting_item):
        if projecting_item['classification'] in ('Otros ingresos y egresos no operativos', 'Otros ingresos y egresos operativos'):
            self.pyg_projected_vectors[projecting_item['classification']] = [projecting_item['value']] * self.projection_dates_len
            return True

        projections = [self.get_historic_summary_values(projecting_item['classification'])[-1]] * self.projection_dates_len
        self.pyg_projected_vectors[projecting_item['classification']] = projections
        return True

    @debugger_wrapper('Error proyectando input', 'Error proyectando un item con proyeccion input')
    def calculate_input(self, projecting_item):
        projections = [float(row['ATRIBUTE']) for row in self.assessment_models['MODEL_PROJECTED_PYG'] if row['ID_RAW_PYG'] == projecting_item['ID_RAW_PYG']]
        logger.debug(f"[calculate_input] estas projectiones estan quedando en cero: {projections}, buscando {projecting_item['ID_RAW_PYG']} en {self.assessment_models['MODEL_PROJECTED_PYG']}")
        self.pyg_projected_vectors[projecting_item['classification']] = projections
        return True


    @debugger_wrapper('Error proyectando porcentaje fijo', 'Error proyectando un item con proyeccion a porcentajes')
    def calculate_fixed(self, projecting_item):
        
        fixed_percentages = [float(interest) for interest in projecting_item["atributes"]]
        projections = [projecting_item["value"] * (1 + fixed_percentages[0]/100)]
        for i in range(1, self.projection_dates_len):
            projections.append(projections[i - 1] * (1 + fixed_percentages[i]/100))

        self.pyg_projected_vectors[projecting_item['classification']] = projections
        return True


    @debugger_wrapper("Error en calculo de proyecciones proporcionales", "Error calculando proyecciones") #si le colocas el debugger_wrapper se sobreescribe el AttributeError('personalizada') que espera master_items_loop
    def master_calculate_dependencies(self, projecting_item):
        vs_item = self.search_vs_item_projections(projecting_item['dependence'])
        if not vs_item:
            return False

        return self.calculate_proportion_projections(projecting_item, vs_item)


    @debugger_wrapper('Error buscando los vs dependientes del item a proyectar', 'Error, es posible que una relación dependiente esté masl descrita')
    def search_vs_item_projections(self, current_item_vs):
        if type(current_item_vs) is str:
            return self.pyg_projected_vectors.get(current_item_vs, False)

        if type(current_item_vs) is list:
            multi_vs_items = list(map(self.search_vs_item_projections, current_item_vs))
            if not all(multi_vs_items):
                return False
            return multi_vs_items


    @debugger_wrapper("Error en el calculo de la proporcion", "Error calculando")
    def calculate_proportion_projections(self, current_item, vs_item):
        proportion_organizer = {'Porcentaje de otra variable':self.calculate_proportion,
                                'Tasas impositivas':self.calculate_impositive,
                                'sum':self.calculate_sum}
        return proportion_organizer[current_item["PROJECTION_TYPE"]](current_item, vs_item)
        

    @debugger_wrapper('Error generando items de sumatorias', 'Error en el calculo de item sumatorios')
    def calculate_proportion(self, current_item, vs_item_vector):
        projections = [current_item["value"]]
        vs_item_vector.insert(0, self.get_historic_summary_values(current_item["dependence"])[-1])
        for i in range(1, self.projection_dates_len + 1):
            try:
                projections.append(projections[i - 1] / vs_item_vector[i - 1] * vs_item_vector[i])
            except Exception as e:
                projections.append(0)

        projections.pop(0)
        vs_item_vector.pop(0)
        self.pyg_projected_vectors[current_item['classification']] = projections
        return True


    @debugger_wrapper('Error generando items de sumatorias', 'Error en el calculo de item sumatorios')
    def calculate_impositive(self, current_item, vs_item_vector):
        logger.info(f'[calculate_impositive] si está entrando al impositivo con los datos {current_item} y {vs_item_vector}')
        projections = []
        for i in range(self.projection_dates_len):
            projections.append(vs_item_vector[i] * float(current_item["atributes"][i])/100)
        
        self.pyg_projected_vectors[current_item['classification']] = projections
        return True


    @debugger_wrapper('Error generando items de sumatorias', 'Error en el calculo de item sumatorios')
    def calculate_sum(self, current_item, _):
        projections = self.calculate_total_vector(current_item['dependence'], current_item['is_sum'], self.pyg_projected_vectors)
        self.pyg_projected_vectors[current_item['classification']] = projections
        return True

    #TODO si este totalizador lo meto en el loop, puede ser buena idea
    @handler_wrapper('Calculando totales de salida con los datos encontrados', 'Totales de proyecciones de pyg calculados correctamente', 'Error calculando totales de proyecciones de pyg', 'Error calculando totales de pyg')
    def calculate_projections_totals(self):
        for key, depen_signs in self.current_pyg_totals.items():
            self.pyg_projected_vectors[key] = self.calculate_total_vector(depen_signs['dependencies'], depen_signs['is_sum'], self.pyg_projected_vectors)
            logger.info(f'[calculate_projections_totals] El vector de totales de {key} obtenido es {self.pyg_projected_vectors[key]}')


    @handler_wrapper('Organizando records de pyg', 'Records de pyg organizados con exito', 'Error organizando records de pyg', 'Error orgnizando tablas de pyg')
    def organize_pyg_records(self):
        try:
            self.pyg_items_records = [asdict(pyg_item_object(item['ID_RAW_PYG'], item['ID_DEPENDENCE'], item['value'], item['PROJECTION_TYPE'], item['COMMENT'])) for item in self.assessment_models['MODEL_PYG_ITEM']]
        except:
            pass

        self.pyg_projection_records = []
        logger.info(f'[organize_pyg_records] este es antes de crear records: {self.pyg_projected_vectors}')
        for classification, proy_vector in self.pyg_projected_vectors.items():
            logger.warning(f'[mira aca] este diccionario no posee depreciacion del periodo: {self.easy_raw_pyg_dict}')
            id_raw_pyg = self.easy_raw_pyg_dict[classification]
            logger.info(f'[mira aca] este objeto no tiene clasification: {self.assessment_models["MODEL_PYG_ITEM"]}')
            atributes_vector = next((item['atributes'] for item in self.assessment_models['MODEL_PYG_ITEM'] if item['classification'] == classification), False)
            if not atributes_vector:
                atributes_vector = [''] * self.projection_dates_len
            for date, value, atribute in zip(self.projection_dates_long, proy_vector, atributes_vector):
                self.pyg_projection_records.append(asdict(pyg_projected_object(id_raw_pyg, date, value, atribute)))
                
        logger.info(f'[organize_pyg_records] Records creados de pyg proyecciones: {self.pyg_projection_records}')


    @handler_wrapper('Emergiendo proyecciones de pyg a la calculadora de pyg', 'Proyecciones emergidas con exito', 'Error emergiendo proyecciones de pyg a calculdora de pyg', 'Error emergiendo resultados de pyg')
    def merge_proyections_to_pyg_vectors(self):
        logger.info(f'[mira aca] parece que aca se metió un no aplica: {self.pyg_projected_vectors}')
        logger.info(f'[mira aca] antes de agregar proyecciones: {self.pyg_values_vectors}')
        for classification, proy_vector in self.pyg_projected_vectors.items():
            try:
                #self.pyg_values_vectors.get(classification, []).extend(proy_vector) #en este
                self.pyg_values_vectors[classification] = self.pyg_values_vectors.get(classification, [])[:self.historic_dates_len] + proy_vector
                proy_hints_vector = [self.pyg_projected_hints.get(classification, 'Posible cuenta no proyectada')] * self.projection_dates_len
                #self.pyg_hints_vectors.get(classification, []).extend(proy_hints_vector) #y en este, no deberìa manejarse extend sino self.pyg_hints_vectors.get(classification, []) = self.pyg_hints_vectors.get(classification, [])[self.historic_dates_len] + proy_hints_vector
                self.pyg_hints_vectors[classification] = self.pyg_hints_vectors.get(classification, [])[:self.historic_dates_len] + proy_hints_vector
            except Exception as e:
                logger.info(f'[merge_proyections_to_pyg_vectors] no se extienden las proyecciones de {classification} al pyg, motivo: {str(e)}')
                continue
        logger.info(f'[mira aca] despues de agregar proyecciones: {self.pyg_values_vectors}')

            
    @handler_wrapper('Calculando totales finales de pyg proyecciones', 'Segundos totales de pyg proyecciones calculados con exito', 'Error calculando totales finales de pyg proyecciones', 'Error calculando totales de proyecciones pyg')
    def pyg_final_projections_recalc(self, save_to_db = True):
        self.calculate_projections_totals()
        self.organize_pyg_records()
        self.merge_proyections_to_pyg_vectors()
        
        if save_to_db:
            self.df_and_upload(self.pyg_items_records, 'PYG_ITEM')
            self.df_and_upload(self.pyg_projection_records, 'PROJECTED_PYG') 
            self.save_assessment_step("CAPEX")
        