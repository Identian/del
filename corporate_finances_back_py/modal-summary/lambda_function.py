#Este servicio debo cambiarlo de nombre

import json
import logging
import sys
import os
import boto3
import pandas as pd
from datetime import datetime

from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db, call_dynamic_engine


#logging.basicConfig() #En lambdas borra este

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event,context):
    sc_obj = script_object(event)
    lambda_response = sc_obj.starter()
    logger.info(f'respuesta final de lambda: \n{lambda_response}')
    return lambda_response


class script_object:
    def __init__(self, event):
        try:
            self.failed_init = False
            self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 
                                               'Access-Control-Allow-Origin': '*', 
                                              'Access-Control-Allow-Methods': '*'}, "statusCode": 500, 'body': {}}
            self.db_connection = 0
            self.detailed_raise = ''
            self.partial_response = list()
            
            logger.warning(f'event de entrada: {str(event)}')
            self.id_assessment = event['queryStringParameters']['id_assessment']
            self.context = event['queryStringParameters']['context']

            self.puc_chapters = {'1':'Activo', '2':'Pasivo', '3':'Patrimonio', '4':'Ingresos', '5':'Gastos', '6':'Costos de venta', '7':'Costos de producción o de operación', '8':'Cuentas de orden deudoras', '9':'Cuentas de orden acreedoras'}
            self.context_table_dict = {'patrimony': 'PATRIMONY_RESULTS', 'other_projections': 'OTHER_MODAL_RESULTS', 'wk': 'WK_RESULTS'}
            
            self.historic_dates = list()
            self.projection_dates = list()
            self.assessment_dates_long = list()
            self.total_asssessment_dates = 0
            self.accounts_projected = list()
            self.account_numbers = list()
            self.found_classifications = list()
            
            self.classification_vector = dict()
            
            #wk:
            self.total_actives = list()
            self.total_pasives = list()
            self.total_directory = {'1': self.total_actives, '2': self.total_pasives}
            self.data_items = list()
            self.wk_results = list()
            self.wk_variation = list()
            
            #pat
            self.social_capital_items = list()
            self.pat_changes_items = list()
            self.classification_summary_vector = dict()
            self.social_capital_contributions = list()
            self.cash_dividends = list()
            self.net_utility_values = list()
            
            #op
            self.total_directory = {'1': self.total_actives, '2': self.total_pasives}




        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.create_conection_to_db()
            self.get_assessment_dates()
            self.get_historic_values()
            self.get_projection_values()
            self.organize_data_items()
            self.organize_classifications_summary()
            if self.context == "wk":
                self.wk_calculate_yearly_totals()
                self.wk_organize_partial_response()
                self.wk_organize_to_db()
                
            if self.context == "patrimony":
                self.pat_classification_summary_builder()
                self.pat_call_pyg_service()
                self.pat_calculate_totals()
                self.pat_organize_partial_response()
                self.pat_organize_to_db()
            
            if self.context == "other_projections":
                self.op_calculate_yearly_totals()
                self.op_organize_partial_response()
                self.op_organize_to_db()
            self.delete_previous_results()
            self.send_summary_to_bd()
            
            call_dynamic_engine(self.id_assessment)
            
            return self.response_maker(succesfull_run = True)
            
        except Exception as e:
            logger.error(f'[starter] Hubieron problemas en el comando de la linea: {get_current_error_line()}')
            return self.response_maker(succesfull_run = False, error_str = (str(e)))

    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_db(self):
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)

    @handler_wrapper('Obteniendo las fechas del proceso de valoración', 'Fechas del proceso de valroación obtenidas con exito', 'Error obteniendo las fechas del proceso de valoración', 'Error obteniendo fechas del proceso de valoración')
    def get_assessment_dates(self):
        query = f"SELECT DATES, PROPERTY FROM ASSESSMENT_DATES WHERE ID_ASSESSMENT = {self.id_assessment} ORDER BY DATES"
        logger.info(f"[get_assessment_dates] Query a base de datos para obtener las fechas utilizadas en el proces de valoración:\n {query}")
        rds_data = self.db_connection.execute(query)
        directory = {'HISTORIC': self.historic_dates, 'PROJECTION': self.projection_dates}
        for date_item in rds_data.fetchall():
            self.assessment_dates_long.append(date_item.DATES.strftime('%Y-%m-%d %H:%M:%S'))
            directory.get(date_item.PROPERTY, []).append(date_item.DATES)
            self.total_asssessment_dates = self.total_asssessment_dates +1

        logger.info(f'[get_assessment_dates] Fechas del proceso de valoración:\nhistoricas: {self.historic_dates}\nproyecciones: {self.projection_dates}')
        

    @handler_wrapper('Obteniendo datos de puc purgados','Datos de puc obtenidos','Error obteniendo datos de puc','Error al buscar datos de puc purgados')
    def get_historic_values(self):
        
        query = f"""SELECT A.ACCOUNT_NUMBER AS account, B.ANNUALIZED AS value, D.CLASSIFICATION, B.ACCOUNT_NAME 
FROM MODAL_WINDOWS A, ASSESSMENT_CHECKED B, ARCHIVE C, RAW_CLASSIFICATION D
WHERE A.ID_ASSESSMENT = B.ID_ASSESSMENT AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER AND B.ID_ARCHIVE = C.ID AND B.ID_RAW_CLASSIFICATION = D.ID
AND A.CONTEXT_WINDOW = "{self.context}" AND A.ID_ASSESSMENT = {self.id_assessment} ORDER BY C.INITIAL_DATE, account""" 
                
        logger.info(f"[get_historic_values] Query a base de datos para obtener los datos de puc historicos calculados:\n{query}")
        rds_data = self.db_connection.execute(query)
        
        self.historic_values = [item._asdict() for item in rds_data.fetchall()]
        logger.info(f'[get_historic_values] Datos historicos de las cuentas usadas en la ventana modal:\n{self.historic_values}')
        
        for item in self.historic_values:
            item['value'] = float(item['value'])
            item['chapter'] = self.puc_chapters.get(item['account'][0], 'Capitulo no encontrado')
            if item['account'] not in self.accounts_projected:
                self.accounts_projected.append(item['account'])
            if item['CLASSIFICATION'] not in self.found_classifications:
                self.found_classifications.append(item['CLASSIFICATION'])
        
        logger.warning(f'[get_historic_values] datos de cuentas post procesamiento inicial:\n{self.historic_values}')


    @handler_wrapper('Obteniendo proyecciones de la ventana modal', 'Proyecciones de la ventana modal obtenidas con exito', 'Error obteniendo proyecciones de la ventana modal', 'Error obteniendo proyecciones')
    def get_projection_values(self):
        query = f"""SELECT A.PROJECTED_DATE, A.ACCOUNT_NUMBER AS account, C.CLASSIFICATION, A.VALUE AS value 
FROM MODAL_WINDOWS_PROJECTED A, ASSESSMENT_CHECKED B, RAW_CLASSIFICATION C, ASSESSMENT D
WHERE A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER AND B.ID_RAW_CLASSIFICATION = C.ID AND B.ID_ARCHIVE = D.ID_ARCHIVE
AND A.ID_ASSESSMENT = B.ID_ASSESSMENT AND A.ID_ASSESSMENT = D.ID
AND A.ID_ASSESSMENT = {self.id_assessment} AND CONTEXT_WINDOW = "{self.context}" ORDER BY PROJECTED_DATE""" 
                
        logger.info(f"[get_projection_values] Query a base de datos para obtener los datos de proyectados de la ventana modal:\n{query}")
        rds_data = self.db_connection.execute(query)
        
        self.projection_values = [item._asdict() for item in rds_data.fetchall()]
        logger.info(f'[get_projection_values] Datos proyectados de las cuentas usadas en la ventana modal:\n{self.projection_values}')
        
        for item in self.projection_values:
            item['value'] = float(item['value'])
            item['chapter'] = self.puc_chapters.get(item['account'][0], 'Capitulo no encontrado')

        logger.warning(f'[get_projection_values] datos de cuentas post procesamiento inicial:\n{self.projection_values}')


    @handler_wrapper('Organizando array de items proyectados', 'Array de items proyectado organizado con exito', 'Error organizando array de items proyectado', 'Error organizando pool de items proyectados')
    def organize_data_items(self):
        for account in self.accounts_projected:
            #logger.info(f'[mira aca] buscando {account} en {self.historic_values}')
            account_name = [item['ACCOUNT_NAME'] for item in self.historic_values if item['account'] == account][-1]
            historic_values = [item['value'] for item in self.historic_values if item['account'] == account]
            projection_values = [item['value'] for item in self.projection_values if item['account'] == account]
            this_account_values_list = historic_values + projection_values
            self.total_directory.get(account[0], []).append(this_account_values_list)
            self.data_items.append({'name': account_name, 'values': {'history': historic_values, 'projections': projection_values}})


    @handler_wrapper('Calculando vectores de totales por clasificación', 'Vectores de totales calculados con exito', 'Error calculando vectores de totales por clasificacion', 'Error calculando summaries de clasificaciones')
    def organize_classifications_summary(self):
        for classification in self.found_classifications:
            classifications_account_len = len(set(item['account'] for item in self.historic_values if item['CLASSIFICATION'] == classification))
            historic_classification_items = [item['value'] for item in self.historic_values if item['CLASSIFICATION'] == classification]
            projection_classification_items = [item['value'] for item in self.projection_values if item['CLASSIFICATION'] == classification]
            logger.warning(f'[mira aca] estos deberían ser vectores completos:\nhistoricos:{historic_classification_items}\nproyecciones{projection_classification_items}')
            self.classification_vector[classification] = [sum(historic_classification_items[i:i + classifications_account_len]) for i in range(0, len(historic_classification_items), classifications_account_len) ]
            
            logger.info(f'[organize_classifications_summary] vector de summary de la clasificacion {classification} antes de meter proyecciones:\n{self.classification_vector[classification]}')
            
            self.classification_vector[classification].extend([sum(projection_classification_items[i:i + classifications_account_len]) for i in range(0, len(projection_classification_items), classifications_account_len) ])
            logger.info(f'[organize_classifications_summary] vector de summary de la clasificacion {classification} despues de meter proyecciones:\n{self.classification_vector[classification]}')
            logger.info(f'[organize_classifications_summary] vector de summary de la clasificacion {classification}{self.classification_vector[classification]}\nDesde los datos historicos:\n{self.historic_values}\nY los datos de proyeccion:\n{self.projection_values} ')
            
            
    #working capital
    @handler_wrapper('Realizando los totales de activos y pasivos para cada año', 'Totales de activos y pasivos realizado con exito', 'Error calculando totales de cada año', 'Error calculando totales de resumen')
    def wk_calculate_yearly_totals(self):
        logger.info(f'[mira acaA]: activos: {len(self.total_actives)} pasivos: {len(self.total_pasives)}')
        if not self.total_actives:
            self.total_actives = [[0]* self.total_asssessment_dates] 
            #estos dos es por si acaso no hay cuentas de capitulos 1 o 2
        if not self.total_pasives:
            self.total_pasives = [[0]* self.total_asssessment_dates]
            
        logger.info(f'[mira acaB]: {self.total_actives}')
        self.total_actives = [sum(year_data) for year_data in zip(*self.total_actives)]
        self.total_pasives = [sum(year_data) for year_data in zip(*self.total_pasives)]
        
        self.wk_results = [i-j for i,j in zip(self.total_actives, self.total_pasives)]
        self.wk_variation.append(0)
        
        for previous_index, year_result in enumerate(self.wk_results[1:]):
            self.wk_variation.append(year_result - self.wk_results[previous_index])
            
        
    @handler_wrapper('Organizando respuesta final', 'Respuesta final organizada con exito', 'Error organizando respeusta final', 'Error creando respesta final')
    def wk_organize_partial_response(self):
        self.historic_dates_short = [date.strftime('%d-%m-%Y') for date in self.historic_dates]
        self.projection_dates_short = [date.strftime('%Y') for date in self.projection_dates]
        self.projection_dates_short[0] = self.projection_dates_short[0] if '-12-' in self.historic_dates_short[-1] else f'Diciembre {self.projection_dates_short[0]}'
        self.partial_response = {'datesHistory': self.historic_dates_short, 
                                    'datesProjections': self.projection_dates_short, 
                                    'data': self.data_items, 
                                    'actives': self.total_actives, 
                                    'pasives': self.total_pasives, 
                                    'workingCapital': self.wk_results, 
                                    'variationWorkingCapital': self.wk_variation}

    @handler_wrapper(f'Organizando los records de capital de trabajo que se llevarán a flujo de caja', 'Records de capital de trabajo organizados', 'Error creando los Records de capital de trabajo', 'Error creando resultados a bd')
    def wk_organize_to_db(self):
        self.records_to_bd = [{'SUMMARY_DATE': i, 'WK_VARIATION': j} for i,j in zip(self.assessment_dates_long, self.wk_variation)]

    #working capital
    
    #patrimony
    @handler_wrapper('Separando clasificaciones de patrimonio', 'Separado de clasificaciones de patrimonio terminada', 'Error separando clasificaciones de patrimonio', 'Error procesando patrimonio')
    def pat_classification_summary_builder(self):
        
        for classification in ['Aportes de capital social u otros', 'Cambios en el patrimonio']:
            historic_items = [item for item in self.historic_values if item['CLASSIFICATION'] == classification]
            accounts = set(item['account'] for item in historic_items)
            if accounts:
                historic_values = [item['value'] for item in historic_items]
                self.classification_summary_vector[classification] = [sum(historic_values[i:i + len(accounts)]) for i in range(0, len(historic_values),len(accounts)) ]
                self.add_proyections_results(accounts, classification)
            else:
                self.classification_summary_vector[classification] = [0] * (len(self.historic_dates) + len(self.projection_dates))
            logger.info(f'[pat_classification_summary_builder] los items:\n{historic_items}\nSumados año a año resultaron en:\n{self.classification_summary_vector[classification]}')
        logger.warning(f'[pat_classification_summary_builder]Resultados de las clasificaciones sumadas año a año:\n{self.classification_summary_vector}')
    
    
    @debugger_wrapper('Error agregando proyecciones a una clasificación', 'Error agregando proyecciones de clasificacion')
    def add_proyections_results(self, accounts, classification):
        logger.info(f'[add_proyections_results] de este listado voy a buscar los accounts:\n{self.projection_values}')
        projection_items = [item for item in self.projection_values if item['account'] in accounts]
        projection_values = [item['value'] for item in projection_items]
        projections_sum_values =  [sum(projection_values[i:i + len(accounts)]) for i in range(0, len(projection_values),len(accounts)) ]
        self.classification_summary_vector[classification].extend(projections_sum_values)
            
    
    @handler_wrapper('Iniciando llamada al servicio calculador de pyg', 'Llamada a calculador de pyg terminada', 'Error llamando al servicio calculador de pyg', 'Error recalculando pyg')
    def pat_call_pyg_service(self):
        data_obj = {"pathParameters": {"id_assessment": self.id_assessment}}
        logger.info(f'[pat_call_pyg_service] Data_obj que se le dispara al pyg retrieve')
        data = json.dumps(data_obj).encode()
        #session = boto3.session.Session()
        #lambda_client = session.client('lambda')
        lambda_client = boto3.client('lambda')
        lambda_slave_pyg_retrieve = os.environ['LAMBDA_SLAVE_FINANZAS_PYG_RETRIEVE']
        
        invoke_response = lambda_client.invoke(FunctionName=lambda_slave_pyg_retrieve, Payload = data)
    
        response_object = json.loads(json.loads(invoke_response['Payload'].read().decode())['body'])
        logger.info(f'[pat_call_pyg_service] objeto de respuesta de la lambda slave:\n{response_object}')
        net_utility_item = next(item for item in response_object['data'] if item['name'] == 'Utilidad neta')
        #self.net_utility_historic_values = [item['value'] for item in net_utility_item['values']['history']]
        #self.net_utility_projected_values = [item['value'] for item in net_utility_item['values']['projection']]
        net_utility_vector = net_utility_item['values']['history'] + net_utility_item['values']['projection']
        self.net_utility_values = [item['value'] for item in net_utility_vector]


    @handler_wrapper('Calculando totales de patrimonio', 'Totales de patrimonio calculados con exito', 'Error calculando totales de patrimonio', 'Error calculando totales de patrimonio')
    def pat_calculate_totals(self):
        self.social_capital_contributions.append(0)
        for index, value in enumerate(self.classification_summary_vector['Aportes de capital social u otros'][1:], start = 1):
            self.social_capital_contributions.append(value- self.classification_summary_vector['Aportes de capital social u otros'][index-1])

        self.cash_dividends.append(self.classification_summary_vector['Cambios en el patrimonio'][0] * -1)
        for index, value in enumerate(self.classification_summary_vector['Cambios en el patrimonio'][1:], start = 1):
            self.cash_dividends.append(self.net_utility_values[index-1] - value + self.classification_summary_vector['Cambios en el patrimonio'][index-1])

        
    @handler_wrapper('Organizando objeto final de patrimonio', 'Objeto final de patrimonio organizado con exito', 'Error organizando objeto final de patrimonio', 'Error organizando respuesta final de patrimonio')
    def pat_organize_partial_response(self):
        self.historic_dates_short = [date.strftime('%d-%m-%Y') for date in self.historic_dates]
        self.projection_dates_short = [date.strftime('%Y') for date in self.projection_dates]
        self.projection_dates_short[0] = self.projection_dates_short[0] if '-12-' in self.historic_dates_short[-1] else f'Diciembre {self.projection_dates_short[0]}'
        self.partial_response = {'datesHistory': self.historic_dates_short, 
                                    'datesProjections': self.projection_dates_short, 
                                    'data': self.data_items, 
                                    'socialCapitalContributions': self.social_capital_contributions,
                                    'cashDividends': self.cash_dividends}

    @handler_wrapper(f'Organizando los records de patrimonio que se llevarán a flujo de caja', 'Records de patrimonio organizados', 'Error creando los Records de patrimonio', 'Error creando resultados a bd')
    def pat_organize_to_db(self):
        logger.info(f'[mira aca] SUMMARY_DATE: {self.assessment_dates_long}, SOCIAL_CONTRIBUTIONS: {self.social_capital_contributions}, CASH_DIVIDENDS: {self.cash_dividends} ')
        self.records_to_bd = [{'SUMMARY_DATE': i, 'SOCIAL_CONTRIBUTIONS': j, 'CASH_DIVIDENDS': k} for i,j,k in zip(self.assessment_dates_long, self.social_capital_contributions, self.cash_dividends)]



    #patrimonio
    
    #otras proyecciones
    @handler_wrapper('Calculando totales de otras proyecciones', 'Totales calculados con exito', 'Error calculando totales de otras proyecciones', 'Error calculando totales')
    def op_calculate_yearly_totals(self):
        #TODO: QUEDÉ ACA: debo sacar el total para la ventana modal de ortas proyecciones
        logger.info(f'[op_calculate_yearly_totals]Summaries de proyecciones encontrados:\n{self.classification_vector}')
        self.operating_cash = self.classification_vector.get('Otros movimientos que no son salida ni entrada de efectivo operativos', [0]*self.total_asssessment_dates)
        self.operating_cash_variation = [item - self.operating_cash[index -1]  for index, item in enumerate(self.operating_cash[1:], start = 1)]
        self.operating_cash_variation.insert(0, 0)
        
        self.fclo = self.classification_vector.get('Otros movimientos netos de activos operativos que afecta el FCLO', [0]*self.total_asssessment_dates)
        self.fclo_variation = [item - self.fclo[index -1]  for index, item in enumerate(self.fclo[1:], start = 1)]
        self.fclo_variation.insert(0, 0)
        
        self.fcla = self.classification_vector.get('Otros movimientos netos de activos operativos que afecta el FCLA', [0]*self.total_asssessment_dates)
        self.fcla_variation = [item - self.fcla[index -1]  for index, item in enumerate(self.fcla[1:], start = 1)]
        self.fcla_variation.insert(0, 0)
        
        logger.info(f'[mira aca]\n{self.operating_cash}\ncon variacion:\n{self.operating_cash_variation}')
        
        
    @handler_wrapper('Organizando objeto final de otras proyecciones', 'Objeto final de otras proyecciones organizado con exito', 'Error organizando objeto final de otras proyecciones', 'Error organizando respuesta final de otras proyecciones')
    def op_organize_partial_response(self):
        self.historic_dates_short = [date.strftime('%d-%m-%Y') for date in self.historic_dates]
        self.projection_dates_short = [date.strftime('%Y') for date in self.projection_dates]
        self.projection_dates_short[0] = self.projection_dates_short[0] if '-12-' in self.historic_dates_short[-1] else f'Diciembre {self.projection_dates_short[0]}'
        self.partial_response = {'datesHistory': self.historic_dates_short, 
                                    'datesProjections': self.projection_dates_short, 
                                    'data': self.data_items, 
                                    'operatingCashInflow': self.operating_cash,
                                    'variationOperatingCashInflow': self.operating_cash_variation,
                                    'fclo' : self.fclo,
                                    'variationFclo' : self.fclo_variation,
                                    'fcla' : self.fcla,
                                    'variationFcla' : self.fcla_variation}
                                     
                                    
    @handler_wrapper(f'Organizando los records de otras proyecciones que se llevarán a flujo de caja', 'Records de otras proyecciones organizados', 'Error creando los Records de otras proyecciones', 'Error creando resultados a bd')
    def op_organize_to_db(self):
        self.records_to_bd = [{'SUMMARY_DATE': i, 'OTHER_OPERATIVE_MOVEMENTS': j, 'FCLO': k, 'FCLA': l} for i,j,k,l in zip(self.assessment_dates_long, self.operating_cash_variation, self.fclo_variation, self.fcla_variation)]

    #otras proyecciones
    
    @handler_wrapper('Eliminando anteriores resultados de la ventana modal', 'Borrado de datos anteriores exitosos', 'Error intentando borrar datos anteriores de ventana modal', 'Error sobreescribiendo datos en bd')
    def delete_previous_results(self):
        query = f"DELETE FROM {self.context_table_dict[self.context]} WHERE ID_ASSESSMENT = {self.id_assessment}"
        logger.warning(f'[delete_previous_results] Query para eliminar datos anteriores de la tabla {self.context_table_dict[self.context]}:\n{query}')
        self.db_connection.execute(query)
    
    
    @handler_wrapper('Enviando resultados a bd', 'Envío de resultados a bd exitoso', 'Problemas enviando resultados a bd', 'Error enviando resultados a bd')
    def send_summary_to_bd(self):
        try: #TODO: quitar este try cuando haya guardado working capital y otras proyecciones
            dataframe_to_bd = pd.DataFrame.from_records(self.records_to_bd)
            dataframe_to_bd['ID_ASSESSMENT'] = self.id_assessment
            logger.warning(f'[send_summary_to_bd] Se cargará la sgte información a la tabla {self.context_table_dict[self.context]}:\n{dataframe_to_bd.to_string()}')
            dataframe_to_bd.to_sql(name = self.context_table_dict[self.context], con=self.db_connection, if_exists='append', index=False)
        except Exception as e:
            logger.error(f'[send_summary_to_bd] Error enviando a bd: {str(e)}')
            pass
        

    @debugger_wrapper('Error construyendo respuesta final', 'Error construyendo respuesta')
    def response_maker(self, succesfull_run = False, error_str = str()):
        if self.db_connection:
            self.db_connection.close()
        if succesfull_run:
            self.final_response['statusCode'] = 200
            self.final_response['body'] = json.dumps(self.partial_response)
            return self.final_response
            
        self.final_response['body'] = json.dumps(self.detailed_raise if self.detailed_raise else error_str)
        return self.final_response



def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    