from decorators import handler_wrapper, timing, debugger_wrapper
import logging
import pandas as pd
import datetime
import sys
from _orchester_utils import acute
from dataclasses_box import capex_summary_object, asdict

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class capex_summary_class(object):
    @handler_wrapper("Actualizando datos de nuevo capex", "Nuevo capex calculado con exito", "Error actualizando resultados de nuevo capex", "Error actualizando capex")
    def capex_summary_recalc(self):
        #if not self.assessment_models.get("MODEL_CAPEX", False):
        #    logger.warning("[new_capex_recalc] No hay atributos para las proyecciones de capex")
        #    self.assessment_projections_found = False
        #    return
        
        self.capex_summary_vectors = {
            "Depreciación del Periodo": {},
            "Depreciación Acumulada": {},
            "Amortización del Periodo": {},
            "Amortización Acumulada": {},
            "brute_assets": {},
        }
        self.dep_am_historic_vectors = {'Depreciación del Periodo': [], 'Depreciación Acumulada': [], 'Amortización del Periodo': [], 'Amortización Acumulada': []}

        self.get_historic_dep_am_data()
        self.calculate_net_value_results()
        self.organize_fixed_assets_results()
        self.calculate_historic_summary_capex()
        logger.info(f"[capex_summary_recalc] Matriz organizada de activos fijos:\n{self.capex_summary_vectors}")
        self.brute_assets_vector = self.capex_summary_vectors.pop("brute_assets")
        self.create_depre_amort_vectors()
        logger.info(f"[capex_summary_recalc] Resultado de vectorización para depreciaciones y amortizaciones:\n{self.capex_summary_vectors}")
        self.create_brute_assets_vector()

        logger.info(f"""[capex_summary_recalc] Resultados de summary:
Ingresos operacionales: {self.operation_income_vector}
Activos brutos: {self.brute_assets_vector}
CAPEX: {self.new_capex_vector}
Depreciación capex: {self.dep_capex}
Depreciación del Periodo: {self.capex_summary_vectors['Depreciación del Periodo']}
Amortización del Periodo: {self.capex_summary_vectors['Amortización del Periodo']}
Depreciación Acumulada: {self.capex_summary_vectors['Depreciación Acumulada']}
Amortización Acumulada: {self.capex_summary_vectors['Amortización Acumulada']}""")
        self.organize_capex_summary_records()
        self.df_and_upload(self.capex_summary_records, "CAPEX_SUMMARY")

        self.save_assessment_step("CAPEX")


    @handler_wrapper('Adquiriendo summaries para las clasificaciones que se requieren en capex', 'totales adquiridos', 'Error adquiriendo summaries que requiere capex', 'Error adquiriendo insumos de capex')
    def get_historic_dep_am_data(self):
        for classification, vector in self.dep_am_historic_vectors.items():
            self.dep_am_historic_vectors[classification] = self.get_historic_summary_values(classification, startswith = True)
            
        acum_vector = []
        for classification in ['Activo Fijo', 'Activos intangibles']:
            acum_vector.append(self.get_historic_summary_values(classification, startswith = True))
        self.fixed_assets_hist_vector = [sum(items) for items in zip(*acum_vector)]


    @handler_wrapper('Vectorizando resultados de activos fijos', 'Valores de activos fijos vectorizados con exito', 'Error vectorizando resultados de depreciación de activos fijos', 'Error trayendo resultados de depreciación de activos fijos a calculo de nuevo capex')
    def calculate_net_value_results(self):
        #logger.warning(f'[vectorize_fixed_assets_results] datos que llegan desde depreciacion de assets:\n{self.fixed_assets_data_to_new_capex}')
        #for proy_year in self.projection_dates:
        #    year_items = [item[str(proy_year)] for item in self.fixed_assets_data_to_new_capex if item.get(str(proy_year), False)]
        #    for key, vector in self.fixed_assets_proy_vectors.items():
        #        vector.append(sum(item[key] for item in year_items))

        self.fixed_assets_proy_vectors['period'] = [i+j for i,j in zip(self.dep_am_historic_vectors['Depreciación del Periodo'], self.dep_am_historic_vectors['Amortización del Periodo'])] + self.fixed_assets_proy_vectors['period']
        self.fixed_assets_proy_vectors['acumulated'] = [i+j for i,j in zip(self.dep_am_historic_vectors['Depreciación Acumulada'], self.dep_am_historic_vectors['Amortización Acumulada'])] + self.fixed_assets_proy_vectors['acumulated']
        self.fixed_assets_proy_vectors['asset'] = self.fixed_assets_hist_vector + self.fixed_assets_proy_vectors['asset']
        self.fixed_assets_proy_vectors['net_value'] = [i-j for i, j in zip(self.fixed_assets_proy_vectors['asset'], self.fixed_assets_proy_vectors['acumulated'])]
        logger.warning(f'[vectorize_fixed_assets_results] vectores resultados: \n{self.fixed_assets_proy_vectors}')
        
    @handler_wrapper('Calculando historico de nuevo capex', 'Historicos de nuevo capex calculado con exito', 'Error calculando historico de nuevo capex', 'Error calculando historicos de nuevo capex')
    def calculate_historic_summary_capex(self):
        self.capex_historic_vector = [0]
        for index, _ in enumerate(self.historic_dates[1:]):
            self.capex_historic_vector.append(self.fixed_assets_proy_vectors['net_value'][index+1] - self.fixed_assets_proy_vectors['net_value'][index] + self.fixed_assets_proy_vectors['period'][index + 1])

        logger.info(f"[calculate_historic_new_capex] historicos de nuevo capex: {self.new_capex_vector}")
        self.new_capex_vector = self.capex_historic_vector + self.new_capex_vector[self.historic_dates_len:]


    @handler_wrapper("organizando información que llega a summary de capex", "Información organizada con exito", "Error organizando información que llega a calculadora de capex summary", "Error construyendo insumos de capex summary")
    def organize_fixed_assets_results(self):
        all_fixed_assets_accounts = set()
        accounts_classificacion_dict = {}
        for _, accounts_dict in self.fixed_assets_data_to_capex_summary.items():
            all_fixed_assets_accounts.update(list(accounts_dict.values()))
        #for account in all_fixed_assets_accounts:
        #    accounts_classificacion_dict[account] = next((item["classification"] for item in self.purged_items if item["account"] == account ), False, )
        logger.info(f"[organize_fixed_assets_results] diccionario de cuentas:clasificaciones:\n{accounts_classificacion_dict}")
        logger.warning(f'[organize_fixed_assets_results] Parece que esto está llegando vacion:\n{self.fixed_assets_proy_to_capex_summary}')
        for proy_row in self.fixed_assets_proy_to_capex_summary:
            logger.debug(f'[organize_fixed_assets_results] {proy_row}')
            if str(proy_row["date"]) not in self.projection_dates_long:
                continue
            proy_accounts = self.fixed_assets_data_to_capex_summary[proy_row["id_items_group"]]
            acumul_classification = self.easy_classification_dict[int(proy_accounts["acumulated_account"])]
            if not acumul_classification:
                self.noting_list.append({"TASK": self.current_context, "NOTE": f"La cuenta {proy_accounts['acumulated_account']} de activos fijos no tiene clasificacion", })
                continue

            period_classification = self.easy_classification_dict[int(proy_accounts["period_account"])]
            if not period_classification:
                self.noting_list.append({"TASK": self.current_context,"NOTE": f"La cuenta {proy_accounts['period_account']} de activos fijos no tiene clasificacion"})
                continue

            self.integrate_proy_row(proy_row, ' '.join(acumul_classification.split(' ')[:-1]), ' '.join(period_classification.split(' ')[:-1]))

    @debugger_wrapper("Error integrando un resultado de proyeccion de activos fijos al diccionario de vectores summary", "Error integramdo activos fijos a summary",)
    def integrate_proy_row(self, proy_row, acumul_classification, period_classification):
        row_year = proy_row["date"]
        if row_year not in self.capex_summary_vectors[acumul_classification]:
            self.capex_summary_vectors[acumul_classification][row_year] = proy_row["acumulated"]
        else:
            self.capex_summary_vectors[acumul_classification][row_year] = (self.capex_summary_vectors[acumul_classification][row_year] + proy_row["acumulated"])

        if row_year not in self.capex_summary_vectors[period_classification]:
            self.capex_summary_vectors[period_classification][row_year] = proy_row["period"]
        else:
            self.capex_summary_vectors[period_classification][row_year] = (self.capex_summary_vectors[period_classification][row_year] + proy_row["period"])

        if row_year not in self.capex_summary_vectors["brute_assets"]:
            self.capex_summary_vectors["brute_assets"][row_year] = proy_row["asset_value"]
        else:
            self.capex_summary_vectors["brute_assets"][row_year] = (self.capex_summary_vectors["brute_assets"][row_year] + proy_row["asset_value"])


    @handler_wrapper("Creando vectores de depreciacion y amortizacion", "vectores de depreciacion y amortización construídos", "Error creando vectores de depreciación y amortización", "Error creando vectores de activos fijos")
    def create_depre_amort_vectors(self):
        for classification, vector in self.capex_summary_vectors.items():
            historic_vector = self.get_historic_summary_values(classification, startswith = True)
            proy_vector = [vector.get(date, 0) for date in self.projection_dates_long]
            self.capex_summary_vectors[classification] = historic_vector + proy_vector


    @handler_wrapper("Creando vector de activos existentes brutos", "Vector de activos existentes brutos creado con exito", "Error creando vector de activos existentes brutos", "Error creando linea de capex summary para activos existentes brutos",)
    def create_brute_assets_vector(self):
        historic_vector = [i + j for i, j in zip(self.get_historic_summary_values("Activo Fijo", startswith = True), self.get_historic_summary_values("Activos intangibles 1", startswith = True))]
        proy_vector = [self.brute_assets_vector.get(date, 0) for date in self.projection_dates_long]
        self.brute_assets_vector = historic_vector + proy_vector


    @handler_wrapper("Organizando records de capex summary", "Records de capex summary organizados con exito", "Error organizando records de capex summary", "Error creando tabla de capex summary",)
    def organize_capex_summary_records(self):
        if not self.new_capex_vector:
            self.new_capex_vector = [0] * self.all_dates_len
            self.dep_capex = [0] * self.all_dates_len
        self.capex_summary_records = [
            asdict(capex_summary_object(*args)) for args in zip(
                self.all_dates_long,
                self.operation_income_vector,
                self.brute_assets_vector,
                self.new_capex_vector,
                self.dep_capex,
                self.capex_summary_vectors["Depreciación del Periodo"],
                self.capex_summary_vectors["Amortización del Periodo"],
                self.capex_summary_vectors["Depreciación Acumulada"],
                self.capex_summary_vectors["Amortización Acumulada"])]
