import json
import logging
import sys
import os
from sqlalchemy import text

if __name__ in ['__main__', 'lambda_function']:
    from __init__ import *
else:
    from .__init__ import *

logging.basicConfig()
logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(module)s] %(message)s', force=True, datefmt='%Y-%m-%d %H:%M:%S')
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
            self.partial_response = {}
    
            logger.warning(f'event de entrada: {str(event)}')
 
            self.puc_chapters = {'1':'Activo', '2':'Pasivo', '3':'Patrimonio', '4':'Ingresos', '5':'Gastos', '6':'Costos de venta', '7':'Costos de producción o de operación', '8':'Cuentas de orden deudoras', '9':'Cuentas de orden acreedoras'}
            self.status_dict = {'No clasificado': False}


            event_dict = event['pathParameters']
            self.id_assessment = event_dict['id_assessment']
            self.all_archives_ids = list()
            self.classification_master = list()
            self.full_accounts = list()
            self.archives_dates = list()
            self.full_merged = list()
            
            
        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    @timing
    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            
            self.create_conection_to_db()
            self.get_user_classification()
            if not self.classification_master:
                self.get_default_sector_classification()
            
            self.get_historic_archives_ids()
            self.get_puc_original_data()
            self.balance_pucs_accounts()
            self.merge_information()
            self.filter_data_by_archive_date()
            
            self.db_connection.close()
            return self.response_maker(succesfull_run = True)
        except Exception as e:
            if self.db_connection:
                self.db_connection.close()
            logger.error(f'[starter] Hubieron problemas en el comando de la linea: {get_current_error_line()}')
            return self.response_maker(succesfull_run = False, error_str = (str(e)))

    @handler_wrapper('Creando conexion a bd','Conexion a bd creada con exito','Error en la conexion a bd','Problemas creando la conexion a bd')
    def create_conection_to_db(self):
        if __name__ != 'lambda_function':
            self.db_connection = connect_to_db_local_dev()
            return
        
        db_schema = os.environ['DB_SCHEMA']
        secret_db_region = os.environ['SECRET_DB_REGION']
        secret_db_name = os.environ['SECRET_DB_NAME']
        db_secret = get_secret(secret_db_region, secret_db_name)
        self.db_connection = connect_to_db(db_schema, db_secret)

    @handler_wrapper('Chequeando si el proceso de valoración posee clasificacion de usuario', 'Chequeo terminado', 'Error chequeando si el proceso de valoración posee clasificación de usuario', 'Error extrayendo clasificación de usuario')
    def get_user_classification(self):
        query = f"SELECT A.ACCOUNT_NUMBER AS account, B.CLASSIFICATION AS classification FROM USER_CLASSIFICATION A, RAW_CLASSIFICATION B WHERE A.ID_RAW_CLASSIFICATION = B.ID AND A.ID_ASSESSMENT = :id_assessment ORDER BY A.ACCOUNT_NUMBER"
        logger.info(f'[get_user_classification] Query para obtener el modelo de clasificaciones del proceso de valoracion:\n{query}')
        rds_data = self.db_connection.execute(text(query), {'id_assessment': self.id_assessment})
        self.classification_master = [row._asdict() for row in rds_data.fetchall()]


    @handler_wrapper('Ya que no hay clasificación de usuario, se requirió usar las clasificaciones por modelo a partir del sector', 'Clasificación por defecto adquirida con éxito', 'Error clasificación por defecto', 'Error extrayendo clasificación por defecto')
    def get_default_sector_classification(self):
        query = f"""SELECT D.ACCOUNT_NUMBER AS account, E.CLASSIFICATION AS classification FROM COMPANY A, ARCHIVE B, ASSESSMENT C, DEFAULT_CLASSIFICATION D, RAW_CLASSIFICATION E
WHERE A.ID = B.ID_COMPANY AND B.ID = C.ID_ARCHIVE AND A.SECTOR = D.SECTOR AND D.ID_RAW_CLASSIFICATION = E.ID AND C.ID = :id_assessment ORDER BY D.ACCOUNT_NUMBER"""
        logger.info(f'[get_default_sector_classification] Query para obtener el modelo de clasificaciones por defecto:\n{query}')
        rds_data = self.db_connection.execute(text(query), {'id_assessment': self.id_assessment})
        self.classification_master = [row._asdict() for row in rds_data.fetchall()]


    @handler_wrapper('Buscando clasificacion master manual', 'Master asignado correctamente', 'Error adquiriendo master de clasificacion','Error adquiriendo clasificacion de analista')    
    def get_historic_archives_ids(self):
        query = f"""SELECT B.ID FROM ARCHIVE B WHERE INITIAL_DATE IN (SELECT D.DATES FROM ASSESSMENT C, ASSESSMENT_DATES D
WHERE C.ID = D.ID_ASSESSMENT AND C.ID = :id_assessment AND D.PROPERTY = "HISTORIC")
AND ID_COMPANY = (SELECT B.ID_COMPANY FROM ARCHIVE B, ASSESSMENT C WHERE B.ID = ID_ARCHIVE AND C.ID = :id_assessment) ORDER BY B.INITIAL_DATE"""
        logger.info(f'[get_historic_archives_ids] Query para obtener los archives seleccionados del proceso de valoracion:\n{query}')
        rds_data = self.db_connection.execute(text(query), {'id_assessment': self.id_assessment})

        self.all_archives_ids = [row.ID for row in rds_data.fetchall()]

        
    @handler_wrapper('Obteniendo datos de pucs originales','Datos adquiridos correctamente','Error obteniendo datos de PUC','Error obteniendo Pucs')
    def get_puc_original_data(self):
        if len(self.all_archives_ids) == 1:
            query = f"SELECT A.INITIAL_DATE AS initial_date, B.ACCOUNT_NUMBER AS account, B.CHECKED_BALANCE AS balance, B.ACCOUNT_NAME AS name FROM ARCHIVE A, ORIGINAL_PUC B WHERE A.ID = B.ID_ARCHIVE AND A.ID IN ({self.all_archives_ids[0]})"
        else:
            query = f"SELECT A.INITIAL_DATE AS initial_date, B.ACCOUNT_NUMBER AS account, B.CHECKED_BALANCE AS balance, B.ACCOUNT_NAME AS name FROM ARCHIVE A, ORIGINAL_PUC B WHERE A.ID = B.ID_ARCHIVE AND A.ID IN {tuple(self.all_archives_ids)}"
        logger.info(f'[get_puc_original_data] Query para obtener los pucs originales del proceso de valoracion: {query}')
        rds_data = self.db_connection.execute(text(query))
        self.full_accounts = [item._asdict() for item in rds_data.fetchall()]
        self.archives_dates = sorted(list(set(item['initial_date'] for item in self.full_accounts))) #aca puedo colocar el inversor de fechas de archives
        
        
    @handler_wrapper('Balanceando numeros de cuentas entre pucs historicos', 'Balanceado de pucs historics exitoso', 'Error balanceando cuentas de pucs historicos', 'Error construyendo cuentas para balanceado de pucs historicos')
    def balance_pucs_accounts(self):
        archives_accounts_directory = {archive: list() for archive in self.archives_dates}
        all_historic_account_numbers = set()
        for row in self.full_accounts:
            all_historic_account_numbers.add(row['account'])
            archives_accounts_directory[row['initial_date']].append(row['account'])
        
        for archive_date, this_archive_accounts in archives_accounts_directory.items():
            this_archive_missing_accounts = [item for item in all_historic_account_numbers if item not in this_archive_accounts]
            logger.warning(f'[mira aca] al puc de la fecha {archive_date} le hacen falta las cuentas {this_archive_missing_accounts}')
            for account_number in this_archive_missing_accounts:
                self.full_accounts.append({'account': account_number, 'name': 'Cuenta creada para balanceo de pucs historicos', 'balance': 0, 'ID_RAW_CLASSIFICATION':0, 'initial_date': archive_date})

        
    @handler_wrapper('Emergiendo informacion a cuentas', 'Información asignada con exito', 'Error emergiendo información a cuentas', 'Error asignando informacion a cuentas')
    def merge_information(self):
        full_accounts_basic_merge = [self.merge_single_account_information(item) for item in self.full_accounts]
        full_accounts_classified = self.merge_classification(full_accounts_basic_merge)
        self.full_merged = [ self.merge_status(item) for item in full_accounts_classified]
        logger.warning(f'zero: {self.full_merged}')

        
    @debugger_wrapper('Error asignando caracteristicas a cuenta', 'Error emergiendo informacion')
    def merge_single_account_information(self, account_dict):
        account_dict['chapter'] = self.puc_chapters.get(account_dict['account'][0])
        account_dict['nivel'] = len(account_dict['account'])
        account_dict['classification'] = 'No aplica'
        
        account_dict['initial_date'] = account_dict['initial_date'].strftime('%Y-%m-%d')
        account_dict['balance'] = float(account_dict['balance'])
        return account_dict
        

    @debugger_wrapper('Problemas asignando clasificaciones', 'Error asignando clasificaciones')
    def merge_classification(self, accounts):
        for master_account in self.classification_master:
            for filtered_account in [account for account in accounts if account['account'].startswith(master_account['account'])]:
                filtered_account['classification'] = master_account['classification']
        return accounts

    #@handler_wrapper('Emergiendo status para el pool de items emergido', 'Status declarado con exito', 'Error emergiendo status', 'Error asignando status con respecto a las clasificaciones')
    @debugger_wrapper('Error emergiendo status para el pool de items emergido', 'Error asignando status con respecto a las clasificaciones')
    def merge_status(self, item):
        item['status'] = self.status_dict.get(item['classification'], True)
        return item




    @handler_wrapper('Filtrando las datas respectivas a las fechas de archives', 'Data preparada para front', 'Error preparando informacion a front, problemas filtrando DATA por fechas de archives', 'Error preparando data')
    def filter_data_by_archive_date(self):
        self.partial_response = []
        for date in self.archives_dates:
            this_short_date = date.strftime('%Y-%m-%d')
            partial_date_data = {'date': this_short_date, 'data': [item for item in self.full_merged if item['initial_date'] == this_short_date]}
            self.partial_response.append(partial_date_data)
        

    @debugger_wrapper('Error construyendo respuesta final', 'Error construyendo respuesta')
    def response_maker(self, succesfull_run = False, error_str = str()):
        if succesfull_run:
            self.final_response['statusCode'] = 200
            self.final_response['body'] = json.dumps(self.partial_response)
            return self.final_response
            
        self.final_response['body'] = json.dumps(self.detailed_raise if self.detailed_raise else error_str)
        return self.final_response

    

def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    

if __name__ == "__main__":
    event = {"pathParameters": {"id_assessment": "2037"}}
    lambda_handler(event, '')