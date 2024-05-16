from decorators import handler_wrapper, timing, debugger_wrapper
import logging
from sqlalchemy import text
import datetime
import numpy as np
import sys

from dataclasses import dataclass, asdict#, field #field es para cuando se debe crear algo en __post_init__
from cash_flow_vars import cash_flow_all_items, cash_flow_totals, pyg_names, capex_items


logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass
class cash_flow_object:
    SUMMARY_DATE        : str
    ID_RAW_CASH_FLOW    : int
    VALUE               : float
    


class cash_flow_class(object):
    @handler_wrapper('Actualizando pantalla de flujo de caja', 'Flujo de caja actualizado con exito', 'Error actualizando flujo de caja', 'Error actualizando flujo de caja')
    def cash_flow_recalc(self):
        self.cash_flow_table = dict()
        self.cash_flow_records = list()
        existance_checker = [{'exists': self.dep_capex, 'function': self.consume_capex_data}, 
                             {'exists': self.patrimony_results_records, 'function': self.consume_patrimony_data}, 
                             {'exists': self.op_results_records, 'function': self.consume_other_projections_data}, 
                             {'exists': self.wk_results_records, 'function': self.consume_working_capital_data}, 
                             {'exists': self.coupled_debt_records, 'function': self.consume_debt_data}]
        
        self.initialize_cash_flow_zero_vectors()
        self.acquire_pyg_values()
        self.consume_pyg_data()

        for existance_function_pair in existance_checker:
            if existance_function_pair['exists']:
                existance_function_pair['function']()
        
        self.get_treasure_rates()
        self.calculate_cash_flow_totals()
        self.calculate_cash_flow()
        self.check_if_trasure_debt()
        
        logger.info(f'[cash_flow_class] Terminación de flujo de caja:\n{self.cash_flow_table}')
        self.create_cash_flow_records()
        
        if self.all_dates_len == self.historic_dates_len:
            return
        self.df_and_upload(self.cash_flow_records, 'CASH_FLOW')
        
        self.save_assessment_step("CASH_FLOW")


    @handler_wrapper('Inicializando vectores en cero', 'Vectores en cero inicializados correctamente', 'Error inicializando vectores en cero de flujo de caja', 'Error creando tabla de flujo de caja')
    def initialize_cash_flow_zero_vectors(self):
        self.cash_flow_table = {item: [0] * self.all_dates_len for item in cash_flow_all_items}


    @handler_wrapper('Obteniendo resultados de pyg', 'Resultados de pyg obtenidos con exito', 'Error obteniendo resultados de pyg', 'Error adquiriendo resultados de pyg')
    def acquire_pyg_values(self):
        logger.info(f'[cash aca] antes de agregar cosas de pyg:\n{self.cash_flow_table}')
        for name in pyg_names:
            self.cash_flow_table[name] = self.pyg_values_vectors[name]
        logger.info(f'[cash aca] despues de agregar cosas de pyg:\n{self.cash_flow_table}')


    @handler_wrapper('Utilizando informacion de pyg para calcular partes de flujo de caja','Pyg aplicado con exito', 'Error aplicando informacion de pyg a flujo de caja','Error aplicando pyg')
    def consume_pyg_data(self):
        self.cash_flow_table['Impuestos operacionales'] = [i*j/k for i,j,k in zip(
                                                                            self.cash_flow_table['EBIT'], 
                                                                            self.cash_flow_table['Impuestos de renta'], 
                                                                            self.cash_flow_table['Utilidad antes de impuestos'])]

        self.cash_flow_table['Impuestos no operacionales'] = [i - j for i,j in zip(self.cash_flow_table['Impuestos de renta'], self.cash_flow_table['Impuestos operacionales'])]
        self.cash_flow_table['UODI (NOPAT)'] = [i * (1- j/k) for i,j,k in zip(self.cash_flow_table['EBIT'], self.cash_flow_table['Impuestos de renta'], self.cash_flow_table['Utilidad antes de impuestos'])]
        logger.info(f'[consume_pyg_data] Resultados de flujo de caja al aplicar datos de pyg:\n{self.cash_flow_table}')
        

    @handler_wrapper('Se encontraron datos de capex, adquiriendo', 'Datos de capex adquiridos con exito', 'Error adquiriendo datos de capx', 'Error adquiriendo dependencias de capex')
    def consume_capex_data(self):
        for item in capex_items:
            self.cash_flow_table[item] = self.capex_summary_vectors[item]
        self.cash_flow_table['CAPEX'] = self.new_capex_vector
        self.cash_flow_table['Depreciación Capex'] = self.dep_capex


    @handler_wrapper('Se encontraron calculos anteriores de patrimonio, consumiendo', 'Calculos de patrimonio consumidos con exito', 'Error utilizando los calculos encontrados de patrimonio', 'Error consumiendo resultados patrimonio')
    def consume_patrimony_data(self):
        self.cash_flow_table['Aportes de capital social u otros'] = [row['SOCIAL_CONTRIBUTIONS'] for row in self.patrimony_results_records]
        self.cash_flow_table['Dividentos en efectivo'] = [row['CASH_DIVIDENDS'] for row in self.patrimony_results_records]


    @handler_wrapper('Se encontraron datos de otras proyecciones, consumiendo', 'Calculos de otras proyecciones consumidos con exito', 'Error consumiendo datos de otras proyecciones', 'Error consumientos resultados de otras proyecciones')
    def consume_other_projections_data(self):
        self.cash_flow_table['Otros movimientos que no son salida ni entrada de efectivo operativos'] = [row['OTHER_OPERATIVE_MOVEMENTS'] for row in self.op_results_records]
        self.cash_flow_table['Otros movimientos netos de activos operativos que afecta el FCLA'] = [row['FCLA'] for row in self.op_results_records]
        self.cash_flow_table['Otros movimientos netos de activos operativos que afecta el FCLO'] = [row['FCLO'] for row in self.op_results_records]


    @handler_wrapper('Se encontraron datos de capital de trabajo, consumiendo', 'Datos de capital de trabajo consumidos con exito', 'Error consumiendo datos de capital de trabajo', 'Error consumientos resultados de capital de trabajo')
    def consume_working_capital_data(self):
        self.cash_flow_table['Capital de trabajo'] = [row['WK_VARIATION'] for row in self.wk_results_records]
        

    @handler_wrapper('Consumiendo datos encontrados de deuda', 'Datos de deuda consumidos con exito', 'Error consumiendo datos de deuda', 'Error consumiendo datos de deuda')
    def consume_debt_data(self):
        self.cash_flow_table['Variación de deuda'] = [row['DISBURSEMENT'] for row in self.coupled_debt_records]
        self.cash_flow_table['Intereses/gasto financiero'] = [row['INTEREST_VALUE'] for row in self.coupled_debt_records]
        self.cash_flow_table['Intereses/gasto financiero FC'] = self.cash_flow_table['Intereses/gasto financiero']

    @handler_wrapper('Chequeando si se asaignaron tasas de deuda de tesorería', 'Chequeo de tasas terminado', 'Error chequeando y asignando deudas de tesorería', 'Error chequeando existencia de deudas de tesorería')
    def get_treasure_rates(self):
        query = """SELECT RATE_ATRIBUTE, SPREAD_ATRIBUTE FROM PROJECTED_DEBT WHERE ID_ASSESSMENT = :id_assessment AND ALIAS_NAME = "Deuda de Tesoreria" ORDER BY ITEM_DATE"""
        logger.info(f"[get_debt_data] Query a base de datos para obtener as tasas de deuda de tesorería:\n {query}")
        rds_data = self.db_connection.execute(text(query), {"id_assessment":self.id_assessment})
        self.treasure_rates = [float(row.RATE_ATRIBUTE) + float(row.SPREAD_ATRIBUTE) for row in rds_data.fetchall()]
        if len(self.treasure_rates) < self.projection_dates_len:
            self.treasure_rates = [0] * (self.projection_dates_len - len(self.treasure_rates)) + self.treasure_rates
        if not self.treasure_rates:
            self.treasure_rates = [0] * self.projection_dates_len


    @handler_wrapper('Calculando totales de salida con los datos encontrados', 'Totales de flujo de caja calculados correctamente', 'Error calculando totales de flujo de caja', 'Error calculando totales')
    def calculate_cash_flow_totals(self):
        for key, depen_signs in cash_flow_totals.items():
            #logger.warning(f'[mira aca] sacando totales de {key} que depende de {depen_signs} desde {self.cash_flow_table}')
            self.cash_flow_table[key] = self.calculate_total_vector(depen_signs['dependencies'], depen_signs['is_sum'], self.cash_flow_table)
            logger.info(f'[calculate_cash_flow_totals] El vector de totales de {key} obtenido es {self.cash_flow_table[key]}')
        


    @handler_wrapper('Organizando flujos de caja', 'Flujos de caja organizados con exito', 'Error organizando flujos de caja', 'Error organizando flujo de caja')
    def calculate_cash_flow(self):
        logger.info(f"[calculate_cash_flow] Generando saldos de caja a partir del flujo de caja del periodo:\n{self.cash_flow_table['Flujo de caja del periodo']}")
        self.cash_flow_table['Saldo de caja final'] = [value for value in self.get_historic_summary_values('Caja')]
        self.cash_flow_table['Saldo de caja inicial'] = [0] + [value for value in self.get_historic_summary_values('Caja')]
        
        for index in range(self.historic_dates_len, self.all_dates_len):
            logger.info(f'valor de index {index}')
            self.cash_flow_table['Saldo de caja inicial'].append(self.cash_flow_table['Saldo de caja final'][index-1])
            self.cash_flow_table['Saldo de caja final'].append(self.cash_flow_table['Saldo de caja inicial'][index] + self.cash_flow_table['Flujo de caja del periodo'][index])

        self.cash_flow_table['Check'] = ['Sí'] * self.all_dates_len


    @handler_wrapper('Chequeando si es necesario agregar deudas de tesorería', 'Chequeo de deudas de tesorería terminado', 'Error calculando deudas de tesorería', 'Error construyendo deudas de tesorería')
    def check_if_trasure_debt(self):
        logger.warning(f'[check_if_trasure_debt] flujos de caja final obtenidos:\n{self.cash_flow_table["Saldo de caja final"]}')
        for proy_year in range(self.projection_dates_len):
            table_vectors_index = self.historic_dates_len + proy_year
            final_cash_flow_current_year = self.cash_flow_table['Saldo de caja final'][table_vectors_index]
            final_cash_flow_current_year = 0 if np.isclose(final_cash_flow_current_year,0) else final_cash_flow_current_year
            if final_cash_flow_current_year < 0:
                self.calculate_year_treasure_debt(proy_year, final_cash_flow_current_year)
            else:
                last_acumulated_treasure = self.cash_flow_table['Deuda de tesorería acumulada'][table_vectors_index - 1]
                self.cash_flow_table['Deuda de tesorería acumulada'][table_vectors_index] = last_acumulated_treasure - final_cash_flow_current_year
                if self.cash_flow_table['Deuda de tesorería acumulada'][table_vectors_index] > 0:
                    self.cash_flow_table['Saldo de caja final'][table_vectors_index] = 0
                else:
                    self.cash_flow_table['Deuda de tesorería acumulada'][table_vectors_index] = 0
                    self.cash_flow_table['Saldo de caja final'][table_vectors_index] = final_cash_flow_current_year - last_acumulated_treasure

        self.pyg_final_projections_recalc()
        self.final_pyg()

    #TODO: quedé solucionando este bug
    @debugger_wrapper('Error calculando la deuda de tesorería de uno de los periodos', 'Error calculando deuda de tesorería')
    def calculate_year_treasure_debt(self, proy_year, final_cash_flow):
        table_vectors_index = self.historic_dates_len + proy_year
        yearly_interest = self.treasure_rates[proy_year]/100 #como el trasure debt lo estoy capturando directo de base de datos, puede que allá está más corto que acá, colocare un try-except para que si se queda corto el vector de intereses, asuma interés de 0
        interest_acum = [yearly_interest]
        for i in range(10):
            interest_acum.append(interest_acum[i] * (1 + yearly_interest))
        this_year_total_interest = interest_acum[-1]
        
        while True:
            logger.info(f"[mira aca] antes de reasignar valor de interés: {self.pyg_values_vectors['Intereses/gasto financiero']}")
            real_balance_to_calculate_interest = self.cash_flow_table['Deuda de tesorería acumulada'][table_vectors_index - 1] - final_cash_flow /2
            this_iteration_interest = real_balance_to_calculate_interest * this_year_total_interest
            this_iteration_debt = this_iteration_interest - final_cash_flow
            self.cash_flow_table['Deuda de tesorería del periodo'][table_vectors_index] = self.cash_flow_table['Deuda de tesorería del periodo'][table_vectors_index] + this_iteration_debt
            self.cash_flow_table['Deuda de tesorería acumulada'][table_vectors_index] = self.cash_flow_table['Deuda de tesorería acumulada'][table_vectors_index-1] + self.cash_flow_table['Deuda de tesorería del periodo'][table_vectors_index]
            self.cash_flow_table['Intereses/gasto financiero FC'][table_vectors_index] = self.cash_flow_table['Intereses/gasto financiero FC'][table_vectors_index] + this_iteration_interest
            self.pyg_values_vectors['Intereses/gasto financiero'] = self.cash_flow_table['Intereses/gasto financiero FC']
            self.pyg_projected_vectors['Intereses/gasto financiero'] = self.cash_flow_table['Intereses/gasto financiero FC'][self.historic_dates_len:]
            
            logger.info(f"[mira aca] despues de reasignar valor de interés: {self.pyg_values_vectors['Intereses/gasto financiero']}")
            self.pyg_final_projections_recalc(save_to_db = False)
            self.final_pyg(save_to_db = False)
            logger.info(f"[mira aca] despues de recalcular pygs valor de interés: {self.pyg_values_vectors['Intereses/gasto financiero']}")
            self.acquire_pyg_values()
            self.calculate_cash_flow_totals()
            
            self.calculate_cash_flow()
            end_iteration_final_cash_flow_value = self.cash_flow_table['Saldo de caja final'][table_vectors_index]
            logger.warning(f'[calculate_year_treasure_debt] revisión de flujo de caja final:\nValor original: {final_cash_flow}\nValor al terminar la iteracion: {end_iteration_final_cash_flow_value}')
            if end_iteration_final_cash_flow_value < final_cash_flow or end_iteration_final_cash_flow_value == final_cash_flow:
                logger.error('[calculate_year_treasure_debt] Las iteraciones de deuda de tesorería se fueron hacia el contrario')
                break
                #TODO: mandar a notas que la deuda está convergiendo hacia el laod equivocado
            final_cash_flow = end_iteration_final_cash_flow_value
            
            if (final_cash_flow > 0 or np.isclose(final_cash_flow,0)):
                break
        


    @handler_wrapper('Emergiendo ids de flujo de caja', 'Ids de flujo de caja emergidos con exito', 'Error emergiendo ids de flujo de caja', 'Error resolvindo tabla de flujo de caja')
    def create_cash_flow_records(self):
        for key in cash_flow_all_items:
            values_vector = self.cash_flow_table[key]
            self.cash_flow_records.extend(asdict(cash_flow_object(date, self.cash_flow_id_dict[key], value)) for date, value in zip(self.all_dates_long, values_vector))


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)