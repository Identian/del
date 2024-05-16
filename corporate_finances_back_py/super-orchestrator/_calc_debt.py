from decorators import handler_wrapper, timing, debugger_wrapper
from dataclasses_box import projected_debt_object, debt_object, coupled_debt_object, asdict
import logging
import pandas as pd
import datetime
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class debt_class():

    @handler_wrapper('Actualizando datos de deuda', 'Datos de deuda calculados con exito', 'Error actualizando resultados de deuda', 'Error actualizando deuda')
    def debt_recalc(self):
        self.debt_info = self.assessment_models['MODEL_DEBT']
        if not self.debt_info:
            logger.warning('[debt_recalc] No se encontró modelo de deuda')
            #self.noting_list.append({'TASK': self.current_context, 'NOTE': "No se halló modelo para calculo de deuda"})
            return
        self.debt_projection_records = list()
        self.debt_records = list()
        
        self.process_all_debts()
        self.calculate_coupled_debt()    
        
        self.df_and_upload(self.debt_records, 'DEBT')
        self.df_and_upload(self.debt_projection_records, 'PROJECTED_DEBT')
        self.df_and_upload(self.coupled_debt_records, 'COUPLED_DEBT')

    def process_all_debts(self):
        current_debts = []
        projected_debts = []
        debt_directory = {'0': projected_debts}
        for debt in self.debt_info:
            debt_directory.get(debt['ACCOUNT_NUMBER'], current_debts).append(debt)

        logger.info(f'[debt_recalc] Deudas repartidas:\nDeudas actuales:{current_debts}\n\nDeudas futuras:{projected_debts}')
        for current_debt in current_debts:
            try:
                self.process_current_debts(current_debt)
            except:
                self.noting_list.append({'TASK': self.current_context, 'NOTE': f"La deuda con numero de cuenta {current_debt['ACCOUNT_NUMBER']} No pudo ser calculada"})
                continue

        for proy_debt in projected_debts:
            try:
                self.process_projection_debts(proy_debt)
            except:
                self.noting_list.append({'TASK': self.current_context, 'NOTE': f"La deuda con alias {current_debt['ALIAS_NAME']} No pudo ser calculada"})
                continue


    @debugger_wrapper('Error calculando una deuda actual', 'Error calculando deuda actual')
    def process_current_debts(self, debt_info):
        debt_info['value'] = self.get_historic_account_values(debt_info['ACCOUNT_NUMBER'])[-1]
        projections_items = [item for item in self.assessment_models['MODEL_PROJECTED_DEBT'] if item['ACCOUNT_NUMBER'] == debt_info['ACCOUNT_NUMBER']]
        logger.debug(f'[process_current_debts] items de la deuda, los rates deberían estar bien: {projections_items}, buscando {debt_info["ACCOUNT_NUMBER"]} en {self.assessment_models["MODEL_PROJECTED_DEBT"]}')
        rate_vector = [safe_of(item['RATE_ATRIBUTE']) for item in projections_items][self.historic_dates_len:]
        spread_vector = [safe_of(item['SPREAD_ATRIBUTE']) for item in projections_items][self.historic_dates_len:] #TODO: En algunos casos este vector se queda corto al modelo de la empresa, pendiente de replicar
        projections_interest_vector = [i + j for i,j in zip(rate_vector, spread_vector)]
        debt_projection_years = int(debt_info['ENDING_YEAR']) - int(debt_info['START_YEAR']) + 1
        
        debt_vectors = self.calculate_current_debt_projections(debt_info['value'], projections_interest_vector) #TODO: por el bug anterior, este está quedando mal
        dates_vector = []
        debt_ending_year = str(int(self.historic_dates_long[-1].split('-')[0]) + debt_projection_years)
        for proy_date in self.projection_dates_long: #acá manejo fechas proyectadas
            dates_vector.append(proy_date)
            if debt_ending_year in proy_date:
                break

        projection_records = [asdict(projected_debt_object(debt_info['ACCOUNT_NUMBER'], debt_info['ALIAS_NAME'], 
        vectors[0], vectors[1], vectors[2], vectors[3], vectors[4], vectors[5], vectors[6], vectors[7], vectors[8])) for vectors in zip(dates_vector, *debt_vectors, rate_vector, spread_vector)]

        for date in self.historic_dates_long: #acá manejo fechas historicas
            logger.info(f'[fechas ingresando] mira aca {date}')
            self.debt_projection_records.append(asdict(projected_debt_object(debt_info['ACCOUNT_NUMBER'], debt_info['ALIAS_NAME'], date, 0, 0, 0, 0, 0, 0, 0, 0)))
        
        logger.info(f'[process_current_debts] records proyectados de la deuda corriente con numero de cuenta {debt_info["ACCOUNT_NUMBER"]}:\n{projection_records}')
        self.debt_projection_records.extend(projection_records)
        self.debt_records.append(asdict(debt_object(debt_info['value'], debt_info['ACCOUNT_NUMBER'], debt_info['ALIAS_NAME'], 
        debt_info['PROJECTION_TYPE'], dates_vector[0][:4], dates_vector[-1], debt_info['DEBT_COMMENT'], debt_info['RATE_COMMENT'], debt_info['SPREAD_COMMENT'])))
        
        
    @debugger_wrapper('Error calculando una de las deudas futuras', 'Error calculando una de las deudas futuras')
    def process_projection_debts(self, proy_debt):
        proy_debt['value'] = float(proy_debt['ORIGINAL_VALUE'])
        projections_items = [item for item in self.assessment_models['MODEL_PROJECTED_DEBT'] if item['ALIAS_NAME'] == proy_debt['ALIAS_NAME']]
        
        rate_vector = [safe_of(item['RATE_ATRIBUTE']) for item in projections_items]
        spread_vector = [safe_of(item['SPREAD_ATRIBUTE']) for item in projections_items]
        projections_interest_vector = [i + j for i,j in zip(rate_vector, spread_vector)]

        debt_vectors = self.calculate_projection_debt_projections(proy_debt['value'], projections_interest_vector)
        if self.context == 'full_recurrency':
            new_start_year = int(self.historic_dates_long[-1].split('-')[0]) + 1 #Voy a asumir que todas las deudas futuras del modelo empiezan un año después del año que se está valorando
            new_ending_year = new_start_year +  int(proy_debt['ENDING_YEAR']) - int(proy_debt['START_YEAR'])
            dates_vector = [datetime.datetime.strptime(str(date_year), '%Y').strftime('%Y-%m-%d %H:%M:%S')  for date_year in range(new_start_year, new_ending_year +1)]
        else:
            dates_vector = [datetime.datetime.strptime(str(date_year), '%Y').strftime('%Y-%m-%d %H:%M:%S')  for date_year in range(int(proy_debt['START_YEAR']),  int(proy_debt['ENDING_YEAR']) +1)]
            
        projection_records = [asdict(projected_debt_object(proy_debt['ACCOUNT_NUMBER'], proy_debt['ALIAS_NAME'], 
        vectors[0], vectors[1], vectors[2], vectors[3], vectors[4], vectors[5], vectors[6], vectors[7], vectors[8])) for vectors in zip(dates_vector, *debt_vectors, rate_vector, spread_vector)]
        
        logger.info(f'[process_projection_debts] records de la deuda proyectada con alias {proy_debt["ALIAS_NAME"]}:\n{projection_records}')
        self.debt_projection_records.extend(projection_records)
        #self.debt_records.append(asdict(debt_object(proy_debt['value'], proy_debt['ACCOUNT_NUMBER'], proy_debt['ALIAS_NAME'], 
        #proy_debt['PROJECTION_TYPE'], proy_debt['START_YEAR'], proy_debt['ENDING_YEAR'], proy_debt['DEBT_COMMENT'], proy_debt['RATE_COMMENT'], proy_debt['SPREAD_COMMENT'])))
        self.debt_records.append(asdict(debt_object(proy_debt['value'], proy_debt['ACCOUNT_NUMBER'], proy_debt['ALIAS_NAME'], 
        proy_debt['PROJECTION_TYPE'], dates_vector[0][:4], dates_vector[-1], proy_debt['DEBT_COMMENT'], proy_debt['RATE_COMMENT'], proy_debt['SPREAD_COMMENT'])))
        
        
    @debugger_wrapper('Error calculando las proyecciones de una deuda', 'Error calculando proyecciones de deuda')
    def calculate_current_debt_projections(self, initial_value, interest_vector):
        projections_years = len(interest_vector)
        yearly_amortization_value = initial_value / projections_years
        disbursement = [0] * projections_years
        amortization = [yearly_amortization_value] * projections_years
        ending_balance_variation = [-yearly_amortization_value] * projections_years
        
        ending_balance = [initial_value - yearly_amortization_value]
        interest_balance = [interest_vector[0] * initial_value / 100]
        initial_balance_vector = [initial_value]
        for index, interest in enumerate(interest_vector[1:], start = 1):
            initial_balance_vector.append(ending_balance[index - 1])
            ending_balance.append(initial_balance_vector[index] - amortization[index])
            interest_balance.append(initial_balance_vector[index] * interest / 100)

        return [initial_balance_vector, disbursement, amortization, ending_balance, interest_balance, ending_balance_variation]
        
        
    @debugger_wrapper('Error calculando las proyecciones de una deuda', 'Error calculando proyecciones de deuda')
    def calculate_projection_debt_projections(self, initial_value, interest_vector):
        projections_years = len(interest_vector) - 1
        
        yearly_amortization_value = initial_value / (projections_years + 1)
        
        disbursement = [initial_value] + [0] * projections_years #Esto pareciera diferente pero en realidad está bien, projections_years está recortado por una posicion
        amortization = [yearly_amortization_value] * (projections_years + 1)
        ending_balance_variation = [initial_value - yearly_amortization_value] + [-yearly_amortization_value] * projections_years #Creo que este está mal, en el otro código dice que debería empezar por el ending_balance en la primera posicion, no por [0]
        
        initial_balance_vector = [initial_value]
        ending_balance = [initial_value - yearly_amortization_value] #este parece que está bien
        interest_balance = [initial_value * interest_vector[0]/100]     #este parece que está bien
        
        for index, interest in enumerate(interest_vector[1:], start = 1):
            initial_balance_vector.append(ending_balance[index - 1])
            ending_balance.append(initial_balance_vector[index] - amortization[index])
            interest_balance.append(initial_balance_vector[index] * interest / 100)     #este parece que está bien
            
        return [initial_balance_vector, disbursement, amortization, ending_balance, interest_balance, ending_balance_variation]
        

    @handler_wrapper('Calculando resultados finales de deuda', 'Resultados finales de deuda calculados con exito', 'Error calculando resultados finales de deuda', 'Error calculando resultados de deuda')
    def calculate_coupled_debt(self):
        self.coupled_debt_partial = {date:{'DISBURSEMENT': float(), 'INTEREST_VALUE': float()} for date in self.projection_dates_long}
        logger.info(f'[mira aca] estos son los datos a los que se les va a procesar deuda: {self.debt_projection_records} y {self.coupled_debt_partial}')
        for row in self.debt_projection_records:
            row['ITEM_DATE'] = str(row['ITEM_DATE'])
            if row['ITEM_DATE'] not in self.coupled_debt_partial:
                continue
            self.coupled_debt_partial[row['ITEM_DATE']]['INTEREST_VALUE'] = self.coupled_debt_partial[row['ITEM_DATE']]['INTEREST_VALUE'] + row['INTEREST_VALUE']
            self.coupled_debt_partial[row['ITEM_DATE']]['DISBURSEMENT'] = self.coupled_debt_partial[row['ITEM_DATE']]['DISBURSEMENT'] + row['ENDING_BALANCE_VARIATION']
        for date in self.projection_dates_long:
            if date not in self.coupled_debt_partial:
                self.coupled_debt_partial[date] = {'DISBURSEMENT': 0, 'INTEREST_VALUE': 0}
        

        self.coupled_debt_records = [asdict(coupled_debt_object(date, 0, historic_interest)) for date, historic_interest in zip(self.historic_dates_long, self.get_historic_summary_values('Intereses/gasto financiero'))]

        self.coupled_debt_records.extend(asdict(coupled_debt_object(date, properties['DISBURSEMENT'], properties['INTEREST_VALUE'])) for date, properties in zip(self.projection_dates_long, self.coupled_debt_partial.values())) 

        
        
def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    
def safe_of(number):
    try:
        return(float(number))
    except:
        return 0