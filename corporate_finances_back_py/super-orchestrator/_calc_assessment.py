
from dataclasses_box import fclo_object, calculated_assessment_object, asdict
from decorators import handler_wrapper, timing, debugger_wrapper
import logging
from datetime import datetime, timedelta
import sys
import math
import numpy as np

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class assessment_class():

    @handler_wrapper('Actualizando ventana de valoración', 'Ventana de valoración recalculada con exito', 'Error actualizando ventana de valoración', 'Error actualizando ventana de valoración')
    def assessment_recalc(self):
        if not self.assessment_models['MODEL_CALCULATED_ASSESSMENT']:
            logger.warning('[assessment_recalc] No se encontró modelo de la pantalla de valoración')
            return
        self.assessment_info = self.assessment_models['MODEL_CALCULATED_ASSESSMENT'][0]
        self.assessment_dates_manager()
        self.flux_discount()
        self.company_value()
        self.assets_passives_adjust()
        self.organize_calculated_assessment_record()
        self.organize_fclo_record()
        
        self.df_and_upload(self.calculated_assessment_record, 'CALCULATED_ASSESSMENT')
        self.df_and_upload(self.fclo_records, 'FCLO_DISCOUNT') 
        
        self.save_assessment_step("VALORATION")


    @handler_wrapper('Acomodando fechas para ajuste de la pantalla de valoración', 'Fechas recalculadas con éxito', 'Error recalculando fechas de la ventana de valoración', 'Error calculando fechas de la ventana de valoración')
    def assessment_dates_manager(self):
        false_user_assessment_date = self.assessment_info['ASSESSMENT_DATE']
        false_assessment_initial_date = self.assessment_info['INITIAL_DATE']
        delting_assessment_time = (false_user_assessment_date - false_assessment_initial_date)
        
        self.user_assessment_date = datetime.strptime(self.assessment_initial_date, '%Y-%m-%d %H:%M:%S') + delting_assessment_time #Fecha de publicacion

        starting_year = max(datetime.strptime(self.assessment_initial_date, '%Y-%m-%d %H:%M:%S'), self.user_assessment_date).replace(day = 1, month = 1)
        self.current_closing_date = starting_year.replace(day = 31, month = 12)

        self.current_half_period = self.user_assessment_date + (self.current_closing_date-self.user_assessment_date)/2
        self.next_half_period = self.current_closing_date + timedelta(days = 180)
        self.next_half_period = self.next_half_period.replace(day = 30, month = 6)
        self.dates_adjust_num = (datetime.combine(self.user_assessment_date, datetime.min.time()).timestamp() - datetime.strptime(self.assessment_initial_date, '%Y-%m-%d %H:%M:%S').timestamp()) /30
        self.dates_adjust_den = (datetime.combine(self.current_closing_date, datetime.min.time()).timestamp() - datetime.strptime(self.assessment_initial_date, '%Y-%m-%d %H:%M:%S').timestamp()) / 30

        if delting_assessment_time.total_seconds() == 0:
            self.dates_adjust = 1 if '-12-' in self.assessment_initial_date else 0
        else:    
            try:
                self.dates_adjust = round(self.dates_adjust_num/(24*3600)) / round(self.dates_adjust_den/(24*3600))
            except Exception as e:
                logger.debug(f'[assessment_dates_manager] El error de esta division de ajuste es: {str(e)}')
                self.dates_adjust = 1
        logger.info(f"""[assessment_dates_manager] Salida de fechas:
Fechas de valoración: {self.user_assessment_date}
Fecha de corte de información: {self.assessment_initial_date}
Cierre periodo flujo actual: {self.current_closing_date}
periodo intermedio de flujos: {self.current_half_period}
Mitad de año, flujo siguiente: {self.next_half_period}
Numerador de ajuste: {self.dates_adjust_num}
Denominador de ajuste: {self.dates_adjust_den}
Ajuste por fechas: {self.dates_adjust}""")
        print('Debug prueba')

    @handler_wrapper('Calculando flujos de descuento', 'Flujos de descuento calculados con exito', 'Error calculando flujos de descuento', 'Error calculando flujos de descuento')
    def flux_discount(self):
        cash_flow_chosen_vector = self.cash_flow_table[self.assessment_info['CHOSEN_FLOW_NAME']]
        self.free_operational_cash_flow = cash_flow_chosen_vector[self.historic_dates_len-1:]
        logger.debug(f'[flux_discount] vector que se va a manejar: {self.free_operational_cash_flow}')
        self.free_operational_cash_flow[0] = self.free_operational_cash_flow[0] if '-12-' in self.historic_dates_long[-1] else self.free_operational_cash_flow[1] - self.free_operational_cash_flow.pop(0)

        self.free_operational_cash_flow[0] = self.free_operational_cash_flow[0] * (1 - self.dates_adjust)
        discount_for_period1 = days360(self.user_assessment_date, self.current_closing_date)
        discount_for_period1 = discount_for_period1/720
        
        discount_for_period2 =  days360(self.current_half_period, self.next_half_period)
        discount_for_period2 = discount_for_period2/360

        if datetime.strptime(self.assessment_initial_date, '%Y-%m-%d %H:%M:%S').year == self.user_assessment_date.year:
            self.discount_for_period = [discount_for_period1, discount_for_period2] + [1] * self.projection_dates_len 
        else:
            self.discount_for_period = [0] + [discount_for_period1, discount_for_period2] + [1] * (self.projection_dates_len-1) 


        self.discount_rates = [float(item['DISCOUNT_RATE'])/100 for item in self.assessment_models['MODEL_FCLO_DISCOUNT']]
        self.discount_factor = [1]
        for index, (i, j) in enumerate(zip(self.discount_rates, self.discount_for_period), start=1):
            self.discount_factor.append(self.discount_factor[index-1] * np.power(1/(1+i), j))
        self.discount_factor = self.discount_factor[1:]

        self.discounted_fclo = [i * j for i,j in zip(self.discount_factor, self.free_operational_cash_flow)]
        
        logger.info(f"""[flux_discount]
Vector de periodo de descuento: {self.discount_for_period}
Tasas de descuento manuales:{self.discount_rates}
Factor de descuento: {self.discount_factor}
Vector de caja libre operacional: {self.free_operational_cash_flow}
FCLO descontado: {self.discounted_fclo}""")


    @handler_wrapper('Calculando valor de la empresa', 'Valor de la empresa calculado con exito', 'Error calculando valor de la empresa', 'Error calcuiando valor de empresa')
    def company_value(self):
        self.vp_flux = round(sum(self.discounted_fclo), 2)
        gradient = float(self.assessment_info['GRADIENT'])/100
        self.normalized_cash_flow = self.free_operational_cash_flow[-1] * (1+gradient)
        self.terminal_value = self.normalized_cash_flow / (self.discount_rates[-1] - gradient)
        self.vp_terminal_value = self.terminal_value * self.discount_factor[-1]
        
        
        self.enterprise_value = float(self.vp_flux + self.vp_terminal_value)
        logger.info(f"""[company_value]
VP flujos: {self.vp_flux}
Flujo de caja normalizado: {self.normalized_cash_flow}
valor terminal: {self.terminal_value}
vp valor terminal: {self.vp_terminal_value}
valor de empresa: {self.enterprise_value}
Gradiente: {gradient}
Tasa de descuento: {self.discount_rates[-1]}
""")


    @handler_wrapper('Calculando ajuste por activos y pasivos', 'Ajuste de activos y pasivos calculado', 'Error calculando ajuste por activos y pasivos', 'Error calculando ajuste por activos y pasivos')
    def assets_passives_adjust(self):
        logger.info(f'mira aca este objeto: {self.assessment_info}')
        if self.assessment_info['ADJUST_METHOD'] == 'Automatico':
            self.non_operational_assets = self.get_historic_summary_values('Efectivo y no operacionales')[-1]
            self.operational_passives = self.get_historic_summary_values('Deuda con costo financiero')[-1]
        else:
            self.non_operational_assets = self.assessment_info['TOTAL_NOT_OPERATIONAL_ASSETS']
            self.operational_passives = self.assessment_info['TOTAL_OPERATIONAL_PASIVES']
        self.financial_adjust = float(self.non_operational_assets - self.operational_passives)
        
        self.patrimony_value = self.enterprise_value + self.financial_adjust
        
        self.stock_value = self.patrimony_value/self.assessment_info['OUTSTANDING_SHARES']
        
        logger.info(f"""[assets_passives_adjust] Resultados de pantalla de valoración:
Activos no operacionales: {self.non_operational_assets}
Pasivos no operacionales: {self.operational_passives}
Ajuste de activos y pasivos: {self.financial_adjust}
valor patrimonial: {self.patrimony_value}
Valor por acción: {self.stock_value} con {self.assessment_info['OUTSTANDING_SHARES']} acciones""")
    @handler_wrapper('Organizando record de CALCULATED_ASSESSMENT', 'Record de calculated assessment construido',  'Error construyendo record de calculated assessment', 'Error actualizando propiedades de ventana de valoraciòn')
    def organize_calculated_assessment_record(self):
        self.calculated_assessment_record = [asdict(calculated_assessment_object(self.user_assessment_date, self.assessment_initial_date, self.current_closing_date, self.current_half_period, self.next_half_period, self.dates_adjust, self.assessment_info['DATES_ADJUST_COMMENT'],
                                        self.assessment_info['CHOSEN_FLOW_NAME'], 'False', 'False', 
                                        self.vp_flux, self.assessment_info['GRADIENT'], self.normalized_cash_flow, str(self.discount_rates[-1]*100), self.terminal_value, str(self.discount_factor[-1]), self.vp_terminal_value, self.enterprise_value,
                                        self.financial_adjust, self.non_operational_assets, self.operational_passives, 'False', 'False',
                                        self.patrimony_value, self.assessment_info['OUTSTANDING_SHARES'], self.stock_value, self.assessment_info['ADJUST_METHOD']))]


    @handler_wrapper('Organizando records de FCLO', 'Records de fclo construidos',  'Error construyendo records de tabla fclo', 'Error actualizando propiedades de FCLO')
    def organize_fclo_record(self):
        self.discount_rates = [item * 100 for item in self.discount_rates]
        self.fclo_records = [asdict(fclo_object(*items)) for items in zip(self.all_dates_long[self.historic_dates_len-1:], self.free_operational_cash_flow, self.discount_for_period, self.discount_rates, self.discount_factor, self.discounted_fclo)]
        

def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    
    
def days360(start_date, end_date, method_eu=False):
    start_day = start_date.day
    start_month = start_date.month
    start_year = start_date.year
    end_day = end_date.day
    end_month = end_date.month
    end_year = end_date.year

    if (start_day == 31 or (method_eu is False and start_month == 2 and (start_day == 29 or (start_day == 28 and start_date.is_leap_year is False)))):
        start_day = 30
    if end_day == 31:
        if method_eu is False and start_day != 30:
            end_day = 1
            if end_month == 12:
                end_year += 1
                end_month = 1
            else:
                end_month += 1
        else:
            end_day = 30

    return (end_day + end_month * 30 + end_year * 360 - start_day - start_month * 30 - start_year * 360)