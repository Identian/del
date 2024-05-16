
from dataclasses_box import fixed_assets_object, fixed_assets_projected_item, asdict
from decorators import handler_wrapper, timing, debugger_wrapper
import logging
import pandas as pd
import datetime
import sys


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class fixed_assets_class(object):
    @handler_wrapper('Actualizando proyecciones de activos fijos', 'Activos fijos proyectados correctamente', 'Error proyectando depreciación de activos fijos', 'Error proyectando activos fijos')
    def fixed_assets_recalc(self):
        if not self.assessment_models['MODEL_FIXED_ASSETS']:
            logger.warning('[fixed_assets_recalc] No se encontraron modelos para depreciación de activos fijos')
            return
        tab_info_records = []
        tab_proy_records = []
        for tab in self.assessment_models['MODEL_FIXED_ASSETS']:
            try:
                self.recalculate_tab_info(tab)
                if tab['PROJECTION_TYPE'] == 'D&A en línea recta':
                    proy_info = self.assets_fixed_increment(tab)
                else:
                    proy_info = self.assets_manual_increment(tab)

                tab_info_records.append(tab)
                tab_proy_records.extend([asdict(item) for item in proy_info])
                self.fixed_assets_data_to_new_capex.extend([item.capex_output() for item in proy_info])
                self.fixed_assets_proy_to_capex_summary.extend([item.capex_summary_output() for item in proy_info])
            except:
                continue
            
        self.df_and_upload(tab_info_records, 'FIXED_ASSETS')
        
        self.df_and_upload(tab_proy_records, 'PROJECTED_FIXED_ASSETS')
        self.save_assessment_step("CAPEX")
        

    @debugger_wrapper('Error calculando pestaña de depreciación de activos fijos', 'Error recalculando pestaña de activos fijos')
    def recalculate_tab_info(self, tab):
        tab['ASSET_ORIGINAL_VALUE'] = self.get_historic_summary_values(self.easy_classification_dict[int(tab['ASSET_ACCOUNT'])])[-1]
        tab['ACUMULATED_ORIGINAL_VALUE'] = self.get_historic_summary_values(self.easy_classification_dict[int(tab['ACUMULATED_ACCOUNT'])])[-1]
        tab['PERIOD_ORIGINAL_VALUE'] = self.get_historic_summary_values(self.easy_classification_dict[int(tab['PERIOD_ACCOUNT'])])[-1]
        self.fixed_assets_data_to_capex_summary.update(fixed_assets_object(tab['ID_ITEMS_GROUP'], 
                                                            tab['PROJECTION_TYPE'], 
                                                            tab['ASSET_ACCOUNT'], 
                                                            tab['ACUMULATED_ACCOUNT'], 
                                                            tab['PERIOD_ACCOUNT'], 
                                                            tab['ASSET_ORIGINAL_VALUE'], 
                                                            tab['ACUMULATED_ORIGINAL_VALUE'], 
                                                            tab['PERIOD_ORIGINAL_VALUE'], 
                                                            tab['PROJECTED_YEARS'], 
                                                            tab['CALCULATION_COMMENT']).capex_summary_output())


    @debugger_wrapper('Error en incremento a tasa fija de activo fijo', 'Error calculando pestaña de activo fijo')
    def assets_fixed_increment(self, tab):
        asset_vector= [tab['ASSET_ORIGINAL_VALUE']] * (tab['PROJECTED_YEARS'] + 2)
        
        acumulated_vector = [tab['ACUMULATED_ORIGINAL_VALUE']]
        existing_vector = [tab['ASSET_ORIGINAL_VALUE'] - tab['ACUMULATED_ORIGINAL_VALUE']]
        period_dep_value = existing_vector[-1] / tab['PROJECTED_YEARS']
        period_vector = [period_dep_value] * (tab['PROJECTED_YEARS'] + 1)
        tab_dates = [self.projection_dates_long[0]]

        if '-12-' not in self.historic_dates_long[-1]:
            delta_period = tab['PERIOD_ORIGINAL_VALUE'] * 12 / int(self.historic_dates_long[-1].split('-')[1]) - tab['PERIOD_ORIGINAL_VALUE']
            acumulated_vector.append(acumulated_vector[0] + delta_period)
            existing_vector.append(asset_vector[1] - acumulated_vector[1])
            period_dep_value = existing_vector[-1] / tab['PROJECTED_YEARS']
            period_vector[1:] = [tab['PERIOD_ORIGINAL_VALUE'] + delta_period] + [period_dep_value] * (tab['PROJECTED_YEARS'])

        acumulated_vector = acumulated_vector + [acumulated_vector[-1] + i * period_dep_value for i in range(1, tab['PROJECTED_YEARS'] + 1)]
        existing_vector = existing_vector + [existing_vector[-1] - i * period_dep_value for i in range(1, tab['PROJECTED_YEARS'] + 1)]
            
        for vector in [asset_vector, period_vector, acumulated_vector, existing_vector]:
            vector.pop(0)
        
        first_projection_year = datetime.datetime.strptime(self.projection_dates_long[0], '%Y-%m-%d %H:%M:%S').replace(day = 1, month = 1)
        tab_dates = tab_dates + [first_projection_year.replace(year = first_projection_year.year + year).strftime('%Y-%m-%d %H:%M:%S') for year in range(1, tab['PROJECTED_YEARS'] + 1)]

        return [fixed_assets_projected_item(tab['ID_ITEMS_GROUP'], date, asset, acumulated, existing, period) 
        for date, asset, acumulated, existing, period in zip(tab_dates, asset_vector, acumulated_vector, existing_vector, period_vector)]

        
    @debugger_wrapper('Error en incremento manual de activo fijo', 'Error calculando pestaña de activo fijo')
    def assets_manual_increment(self, tab):
        tab_dates = [self.projection_dates_long[0]]
        first_projection_year = datetime.datetime.strptime(self.projection_dates_long[0], '%Y-%m-%d %H:%M:%S').replace(day = 1, month = 1)
        tab_dates = tab_dates + [first_projection_year.replace(year = first_projection_year.year + year).strftime('%Y-%m-%d %H:%M:%S') for year in range(1, tab['PROJECTED_YEARS'] + 1)]

        period_vector = [float(item['PERIOD_VALUE']) for item in self.assessment_models['MODEL_PROJECTED_FIXED_ASSETS'] if item['ID_ITEMS_GROUP'] == tab['ID_ITEMS_GROUP']]
        period_vector = period_vector if len(period_vector) >= self.projection_dates_len else period_vector + [0] * self.projection_dates_len
        asset_vector= [tab['ASSET_ORIGINAL_VALUE']] * len(tab_dates)
        acumulated_vector = [tab['ACUMULATED_ORIGINAL_VALUE']]  #pop de index 0
        existing_vector = [tab['ASSET_ORIGINAL_VALUE']]         #pop de index 0
        a = 0
        if '-12-' not in self.historic_dates_long[-1]:
            delta_period = tab['PERIOD_ORIGINAL_VALUE'] * 12 / int(self.historic_dates_long[-1].split('-')[1]) - tab['PERIOD_ORIGINAL_VALUE']
            acumulated_vector.append(acumulated_vector[0] + delta_period)
            existing_vector.append(asset_vector[1] - acumulated_vector[1])
            a = 1

        for proy_index, _ in enumerate(tab_dates):
            try:
                acumulated_vector.append(acumulated_vector [-1] + period_vector[proy_index + a])
                existing_vector.append(asset_vector[0] - acumulated_vector[-1])
            except:
                self.noting_list.append({"TASK": self.current_context, "NOTE": f"El tab con cuentas {tab['ASSET_ACCOUNT']}, {tab['ACUMULATED_ACCOUNT']} y {tab['PERIOD_ACCOUNT']}; tuvo una terminación de fechas imprevista"})
                break

        acumulated_vector.pop(0)
        existing_vector.pop(0)
        return [fixed_assets_projected_item(tab['ID_ITEMS_GROUP'], date, i, j, k, m) for date, i, j, k, m in zip(tab_dates, asset_vector, acumulated_vector, existing_vector, period_vector)]

        
        
        
def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)