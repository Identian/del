""":
capas:
ninguna

variables de entorno:
ninguna 

RAM: 256 MB 
"""

import boto3
from datetime import datetime
import json
import logging
from statistics import mean, stdev, median # linear_regression,
import sys
import traceback

from decorators import handler_wrapper, timing

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    lo = lambda_object(event)
    return lo.starter()
    

class lambda_object():
    accountPyg = list
    accountProportion = list
    
    accountPyg_name = str
    accountProportion_name = str
    
    retrieved_long_dates= list
    retrieved_short_dates = list
    
    implicit_growing_results = dict()
    
    final_response = {'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'}}
    
    @handler_wrapper('Obteniendo valores clave de event', 'Valores de event obtenidos correctamente',
               'Error al obtener valores event', 'Fallo en la obtencion de valores clave de event')
    def __init__(self, event) -> None:

        event_body_json = event["body"]
        event_body_dict = json.loads(event_body_json)
        logger.info(f'Objeto de entrada event: {str(event)}')
        self.accountPyg = event_body_dict['accountPyg']
        self.accountPyg_name = self.accountPyg[0]['account']
        self.accountProportion = event_body_dict["accountProportion"]
        self.accountProportion_name = self.accountProportion[0]['account']


    def starter(self):
        try:
            logger.info(f'[starter] Empezando starter de objeto lambda')
            self.get_all_dates()
            self.organize_values_by_date()
            self.calculate_implicit_growing()
            self.calculate_proportions()
            self.get_statistics()
            self.prepare_values_as_strings()
            logger.info(f'[starter] Tareas de starter terminadas con exito')            
            return self.response_maker(succesfull = True)
        except Exception as e:
            logger.error(f'[starter] Error en el procesamieno del comando de la linea: {get_current_error_line()}, motivo: {e}')
            return self.response_maker(succesfull = False, exception_str = e)

    @handler_wrapper('Organizando fechas','Fechas organizadas correctamente','Error al organizar fechas','Error en los datos recibidos')
    def get_all_dates(self):
        dates_AccountPyg = {datetime.strptime(item['date'], '%d-%m-%Y') for item in self.accountPyg}
        self.retrieved_long_dates = sorted(dates_AccountPyg, reverse = False) #Este le da orden ascendente o descendente a todas las fechas a revisar
        
        short_dates = []
        for date in self.retrieved_long_dates:
            short_dates.append(date.strftime('%d-%m-%Y'))
        
        self.retrieved_short_dates = short_dates
        
    handler_wrapper('Organizando valores por fechas','Valores organizados correctamente','Error en la oganizacion de valores','Error en los datos recibidos')
    def organize_values_by_date(self):
        #logger.warning(f'[mira aca fechas cortas] {str(self.retrieved_short_dates)}')
        new_accountPyg = []
        new_accountProportion = []
        for date in self.retrieved_short_dates:
            for item in self.accountPyg:
                if item['date'] == date:
                    item['value'] = abs(item['value'])
                    new_accountPyg.append(item)
            for item in self.accountProportion:
                if item['date'] == date:
                    item['value'] = abs(item['value'])
                    new_accountProportion.append(item)

        self.accountPyg = new_accountPyg
        self.accountProportion = new_accountProportion


    @handler_wrapper('Calculando tabla de variacion','Tabla de variacion calculada con exito','Error en el calculo de tabla de variacion','Error en el calculo de datos')
    def calculate_implicit_growing(self):
        accounts = [self.accountPyg, self.accountProportion]
        variations = []
        index = 0
        
        
        for account in accounts:
            variations.append({'name':'Variación absoluta '+account[0]['account'],'value':['No aplica']})
            variations.append({'name':'Variación relativa '+account[0]['account'],'value':['No aplica']}) 
            for j, item in enumerate(account):
                current_value = item['value']
                try:
                    next_value = account[j+1]['value']
                except Exception:
                    break
                variations[index]['value'].append(next_value-current_value)
                if current_value ==0:
                    variations[index+1]['value'].append(0)
                    continue
                variations[index+1]['value'].append(next_value/current_value - 1)
            index = index + 2
            
        #logger.warning(f'resultado de variations : {variations}')
        self.implicit_growing_results['variations'] = variations
        self.accountPyg_rates = variations[1]['value'][1:]
        self.accountProportion_rates = variations[3]['value'][1:]
        

    def calculate_proportions(self):
        proportions = {'name':f"{self.accountPyg[0]['account']} / {self.accountProportion[0]['account']}", 'value':[]}
        for i in range(len(self.accountPyg)):
            if self.accountProportion[i]['value'] ==0:
                proportions['value'].append('No aplica')
                continue
            proportions['value'].append(self.accountPyg[i]['value'] / self.accountProportion[i]['value'])
        
        self.implicit_growing_results['proportions'] = [proportions]
        
    @handler_wrapper('Calculando valores estadisticos','Valores estadisticos obtenidos','Error calculando valores estadisticos','Problemas en los calculos')
    def get_statistics(self):
    
        account_rates = [self.accountPyg_rates, self.accountProportion_rates]
        account_names = [self.accountPyg_name, self.accountProportion_name]
        statistics = []
        i=0
        
        
        for account in account_rates:
            logger.warning(f'Valor de account: {account}')
            temp_object = {'account' : account_names[i]}
            try:
                temp_object['average'] = mean(account)
            except Exception:
                temp_object['average'] = 'No aplica'
            
            try: 
                temp_object['median'] = median(account)
                temp_object['min'] = min(account)
                temp_object['max'] = max(account)
            except Exception:
                temp_object['median'] = 'No aplica'
                temp_object['min'] = 'No aplica'
                temp_object['max'] = 'No aplica'
            try:
                temp_object['deviation'] = stdev(account)
                temp_object['beta'] = self.calculate_linear_regression()
            except Exception: 
                temp_object['deviation'] = 'No aplica'
                temp_object['beta'] = 'No aplica'
             
            statistics.append(temp_object)
            i = i+1
            
        self.implicit_growing_results['statistics'] = statistics
        ##

    def calculate_linear_regression(self):
        x = self.accountPyg_rates
        y = self.accountProportion_rates
        
        n = len(x)
        ex = sum(x)
        ey = sum(y)
        
        ex2 = sum([item*item for item in x])
        exy = sum([x[i]*y[i] for i in range(n)])

        slope  = (n*exy-(ex*ey))  /  (n*ex2 - (ex**2))
        return slope

    @handler_wrapper('Alistando strings para front','Strings para front alistados correctamente','Error al modificar resultados como strings','Error en el postprocesamiento de informacion')
    def prepare_values_as_strings(self):
        for index, item in enumerate(self.implicit_growing_results['variations']):
            if index %2 == 0:
                self.implicit_growing_results['variations'][index]['value'] = self.prepare_list_as_string(self.implicit_growing_results['variations'][index]['value'], 'coin')
            else:
                self.implicit_growing_results['variations'][index]['value'] = self.prepare_list_as_string(self.implicit_growing_results['variations'][index]['value'], 'percentage')
        
        self.implicit_growing_results['proportions'][0]['value'] = self.prepare_list_as_string(self.implicit_growing_results['proportions'][0]['value'], 'percentage')
 
        for index, statistics_object in enumerate(self.implicit_growing_results['statistics']):
            for key, value in statistics_object.items(): 
                self.implicit_growing_results['statistics'][index][key] = self.prepare_item_as_string(value,'percentage')
 
         
    def prepare_list_as_string(self,array,percentage_coin):
        temp_item = []
        for item in array:
            temp_item.append(self.prepare_item_as_string(item,percentage_coin))
        return temp_item 
        
        
    def prepare_item_as_string(self,item,percentage_coin):
        if percentage_coin == 'coin':
            try:
                return f'$ {item:,.2f}'.replace('.',';').replace(',','.').replace(';',',')
            except Exception:
                return str(item)

        elif percentage_coin == 'percentage':
            try:
                return f'{item*100:.2f} %'
            except Exception:
                return str(item)


    def response_maker(self, succesfull = False, exception_str = str):
        if not succesfull:
            self.final_response['body'] = json.dumps(exception_str)
        else:
            self.final_response['body'] = json.dumps(self.implicit_growing_results)
        return self.final_response



def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)


def get_especific_error_line():
    _, _, exc_tb = sys.exc_info()
    return str(traceback.extract_tb(exc_tb)[-1][1])