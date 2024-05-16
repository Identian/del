""":
=============================================================

Nombre: lbd-dev-etl-fincor-puc-columns-depurate-post
Tipo: Lambda AWS
Autor: Jeisson David Botache Yela
Tecnología - Precia

Ultima modificación: 11/07/2022

Para el proyecto Delta de finanzas corporativas se construyó la lambda 
lbd-dev-layer-fincor-puc-columns-name-get, que sirve para encontrar los encabezados
de columnas de puc, el analista escoge entre estos encabezados cuáles desea utilizar
como columna de nombres de cuenta y columna de saldos. Para evitar problemas por
columnas con el mismo nombre, la información debe llegar a este depurador en una 
estructura especifica.
Requerimientos:
capas
capa-pandas-data-transfer 

variables de entorno:
ARCHIVES_TABLE : ARCHIVE
BUCKET_NAME : s3-dev-datalake-sources
COMPANIES_TABLE : COMPANY
DB_SCHEMA : src_corporate_finance
PUC_BUCKET_PATH : corporate_finances/pucs/client/
SECRET_DB_NAME : precia/rds8/sources/finanzas_corporativas
SECRET_DB_REGION : us-east-1

RAM: 1024 MB

==============================
"""
import boto3
import boto3
import datetime
import json
import logging
import urllib
from queue import Queue
from threading import Thread
import sys
import sqlalchemy
import traceback
import pandas as pd

from decorators import handler_wrapper, timing
from utils import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """:
    Funcion lambda_handler que se activa automaticamente. La función llama los demás metodos de la lambda
    que contienen la logica necesaria para obtener los resultados. Si en el procedimiento hay errores
    se dispara un raise que ejecuta una respuesta base que debería servir para encontrar la ubicacion
    del error.
    :param event: Se puede interpretar como variables base que activan la lambda
    :param context: contiene informacion relevante al ambiente de la lambda, raramente se usa
    :returns: La funcion debe devolver un objeto con los encabezados y el body en la estructura esperada;
    """
    logger.info(f'event de entrada: \n{str(event)}')
    sc_obj = script_object(event)
    return sc_obj.starter()
    
    
class script_object:

    def __init__(self, event):
        try:
            logger.info('[__INIT__] Inicializando objeto lambda ...')
            self.failed_init = False
            logger.info(f'event de entrada: {str(event)}')
            self.bucket_name = os.environ['BUCKET_NAME']
            self.puc_bucket_path = os.environ['PUC_BUCKET_PATH']
            self.companies_table = os.environ['COMPANIES_TABLE']
            self.archives_table = os.environ['ARCHIVES_TABLE']
            db_schema = os.environ['DB_SCHEMA']
            secret_db_region = os.environ['SECRET_DB_REGION']
            secret_db_name = os.environ['SECRET_DB_NAME']
            db_secret = get_secret(secret_db_region, secret_db_name)
            self.db_connection = connect_to_db(db_schema, db_secret)
            
            event_body_json = event["body"]
            event_body_dict = json.loads(event_body_json)
            self.row = event_body_dict['row']
            self.accounts_column = event_body_dict['accounts_column']
            self.nit = event_body_dict['nit']
            self.replace = event_body_dict['replace']
            self.filename = '.'.join(urllib.parse.unquote(event_body_dict['filename'].encode('utf8')).split('.')[:-1]) + '.json'
            self.selected_columns = event_body_dict['selected_columns']
            logger.warning(f'selected columns encontrados: {self.selected_columns}')
            self.s3_client = boto3.client('s3')
            self.company_info = dict()
            self.found_archives = []
            self.repeated_dates = []
            self.depurated_dfs_list = []
            self.queue_inputs = Queue()
            self.queue_outputs = Queue()
            
            self.df_full_original = pd.core.frame.DataFrame()
            
            self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'}} #,'statusCode': 200
            logger.info('[__INIT__] Objeto lambda inicializada exitosamente')
            
        except Exception as e:
            self.failed_init = True
            logger.error(f"[__INIT__] error en inicializacion, linea: {get_especific_error_line()}, motivo: "+str(e)) 


    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Falla inicializacion, revisar logs')
            self.get_company_info()
            repeated_data = self.check_archives_info()
            is_able_to_continue = self.check_values_to_continue(repeated_data)
            if not is_able_to_continue:
                return self.response_maker(succesfull=True)
 
            self.download_file()
            self.read_json_as_dataframe()
            self.depurate_dataframe()
            self.multi_uploader()
            logger.info('Tareas de lambda terminadas con exito')
            self.db_connection.close()
            return self.response_maker(succesfull = True)
        
        except Exception as e:
            if self.db_connection:
                self.db_connection.close()
            logger.error(f'[starter] Error en el procesamieno del comando de la linea: {get_current_error_line()}, motivo: {e}')
            return self.response_maker(succesfull = False, exception_str = e)


    @handler_wrapper('Buscando id del nit recibido','El id del nit recibido fue encontrado','Error en la busqueda de los datos de la empresa','Error, problemas localizando la informacion de la empresa')
    def get_company_info(self):
        query = f"SELECT * FROM {self.companies_table} WHERE NIT=\"{self.nit}\" LIMIT 1"
        logger.info(f'[get_company_info] Query para obtener datos de la empresa: {query}')
        rds_data = self.db_connection.execute(query)
        self.company_info = dict(rds_data.mappings().all()[0])
        
    
    @handler_wrapper('Chequeando si alguno de los archive ya existe','existencia de Archives completada', 'Error chequeando existencia de archives', 'Error fatal, no se pudo confirmar existencia de archives')
    def check_archives_info(self):
        repeated_data = False
        for column_info in self.selected_columns[1:]:
            initial_date = datetime.datetime.strptime(column_info['date'], "%d-%m-%Y").strftime('%Y-%m-%d %H:%M:%S')
            query = f"SELECT ID FROM {self.archives_table} WHERE ID_COMPANY={self.company_info['ID']} AND INITIAL_DATE=\"{initial_date}\" AND PERIODICITY=\"{column_info['periodicity']}\" LIMIT 1"
            logger.warning(f'query para mirar si existe el archive: {query}')
            rds_data = self.db_connection.execute(query)
            if rds_data.rowcount != 0:
                self.repeated_dates.append(column_info['date'])
                repeated_data = True
                found_archive = dict(rds_data.mappings().all()[0])['ID']
                logger.warning(f'Archive encontrado: {found_archive}')
                self.found_archives.append({'found_archive':found_archive, 'date' : initial_date, 'sector':self.company_info['SECTOR']})
                continue
            self.found_archives.append({'company_id':self.company_info['ID'],'date':initial_date,'periodicity':column_info['periodicity'], 'sector':self.company_info['SECTOR']})
    
        logger.info(f'[check_archives_info] Repeated_dates: {self.repeated_dates}')
        return repeated_data


    @handler_wrapper('Chequeando si se puede seguir ejecutando la lamba con datos repetidos','Chequeo terminado','Error al chequear valores para continuar','Error interno')
    def check_values_to_continue(self, repeated_data):
        """:
        Este metodo asume algo importante: unicamente el perfil de 'Especialista' va a poder aceptar un reemplazo en el front,
        entonces, si hay data repetida se va a cancelar la lambda siempre, a menos que el front haya enviado un replace, lo
        cual, unicamente va a poder hacerlo un 'especialista'. hice un metodo completo para chequear esto en caso de que esta
        comprobacion tenga más dificultad en un futuro
        :key_values: De acá necesito ver si hay o no un replace
        :repeated_data: Necesito ver si se encontró el mismo archive en uso, en caso de que no, la lambda va a continuar 
        para cualquier usuario
        """
        if repeated_data and not self.replace:
            return False
        return True

    @handler_wrapper('Descargando archivo desde s3','Descarga exitosa','Error en la descarga del archivo','Hubieron problemas descargando el archivo cargado')
    def download_file(self):
        """:
        Funcion que toma el filename para descargar el archivo de mismo nombre al /tmp del lambda;
        importante: una lambda automatica lee el archivo que se sube a s3 y hace una copia a json.
        :bucket_name: variable de entorno indicando el bucket donde se encuentra el archivo
        :puc_bucket_path: variable de entorno indicando el key path donde se encuentra el archivo
        :filename: nombre del archivo a buscar
        :returns: La funcion no regresa nada
        """
        logger.info(f'[download_file] Descargando archivo: {self.filename} desde {self.puc_bucket_path}...')
        self.s3_client.download_file(self.bucket_name, self.puc_bucket_path+self.filename,'/tmp/'+self.filename)


    @handler_wrapper('Leyendo archivo','Archivo convertido exitosamente','Error en la lectura del json a DataFrame','Error fatal en la lectura del archivo')
    def read_json_as_dataframe(self):
        """:
        Funcion que toma el filename para hacer la lectura del archivo json descargado como DataFrame
        :filename: nombre del archivo a buscar
        :returns: La funcion regresa el dataframe del json. ya que el excel no sufrió cambios al 
        reformatearse a json lo nombro df_full_original
        """
    
        self.df_full_original = pd.read_json('/tmp/'+self.filename, orient="table")
        
    
    @handler_wrapper('Depurando dataframe','Dataframe depurado con exito','Error al depurar Dataframe','Erro fatal en la depuracion')
    def depurate_dataframe(self):
        """:
        Funcion que toma el dataFrame con los datos originales y lo depura por medio de los siguientes key values:
        las variables headers_row y accounts_column se calcularon en la lambda 'lbd-dev-layer-fincor-puc-columns-name-get'
        pero como la comunicacion entre lammbdas debe hacerse entre servicios como RDS, se decidió que el front guardara 
        estas variables para entregarlas en esta lambda. El DataFrame se depura y pasa por una acomodación extra:
        se generalizan los nombres de coilumna a 'account', 'name', 'balance'
        los tipos de datos son todos string excepto el de balance, que es float
        los codigos pasan por un strip en caso de que hayan espacios
        cambio celdas vacias (NaN) por espacios vacios ''
        reviso que las celdas en account sean de tipo numerico, y me quedo con las que cumplen
        organizo el dataframe por account (no olvidar que son numeros pero lo leo como string)
        :headers_row: numero de fila donde se encontró que empieza el puc
        :account_column: numero de columna donde se encontraron los codigos cuenta
        :selected_columns: array de columnas seleccionadas por el analista en el front
        :returns: La funcion regresa el dataframe depurado y con indexes recalculados
        """
        
        columnsIndexes = []
        columnsIndexes.append(self.accounts_column)
        columnsIndexes.append(self.selected_columns.pop(0)['column'])
    
        base_df = self.df_full_original[self.row:].iloc[:, columnsIndexes]
        
        base_df.rename(columns={base_df.columns[0]: 'account', base_df.columns[1]: 'name'}, inplace=True)
        base_df = base_df.astype({"account": "string", "name": "string"}, copy=True)
        base_df['account'] = base_df['account'].str.strip() #hago strip de toda la columna account

        for column_info in self.selected_columns:
            logger.info(f"[depurate_dataframe] depurando la columna: {column_info['column']}, con fecha: {column_info['date']}")
            temp_df = base_df.copy()
            temp_df.fillna('No aplica', inplace=True)
            
            temp_df['balance'] = self.df_full_original[self.row:].iloc[:, column_info['column']]
            #logger.info(f"esto es lo que voy a meter a depurated_dfs_list: {str({'dataframe':temp_df,'archive':self.found_archives.pop(0)})}")
            self.depurated_dfs_list.append({'dataframe':temp_df.to_dict('records'),'archive':self.found_archives.pop(0)})
        
    
    @handler_wrapper('Iniciando multi uploader','Multi uploader terminado con exito','Error en la carga de archivos, por favor revise su base de datos con la data que se intentó subir','Error en la carga a base de datos')
    def multi_uploader(self):
        """:
        Funcion que toma el dataFrame depurado para guardarlo en 
        :filename: cómo se va a nombrear el archivo guardado
        :depurated_file_df: DataFrame depurado
        :returns: La funcion no regresa nada
        """
        for item in self.depurated_dfs_list:
            self.queue_inputs.put(item)
            logger.warning('items guardados en q inputs')
    
        for t in range(len(self.depurated_dfs_list)):
            worker = Thread(target=self.to_db_call)
            worker.daemon = True
            worker.start()
            logger.warning('[multi_uploader] Empiezan a ejecutarse los hilos')
    
        self.queue_inputs.join()
        
        self.final_response['body'] = 'Carga y chequeo de datos exitoso' #,'statusCode': 200}
        results = []
        
        for t in range(len(self.depurated_dfs_list)):

            current_result =self.queue_outputs.get() 
            results.append(current_result)
            if not current_result['succesfull']:
                self.final_response['body'] = 'Hubieron problemas en la carga de algunos datos, porfa comuniquese con un administrador de tecnologia'
            
        self.queue_inputs = ''
        self.queue_outputs = ''

        print(f'Estos fueron los resultados: {results}')


    @handler_wrapper('iniciando hilo de carga de dataframe depurado','hilo terminado','error en el hilo','Error en la carga, confirme la carga en bd')
    def to_db_call(self):
        input_item = self.queue_inputs.get()
        
        data = json.dumps({'body': json.dumps(input_item)}).encode()
        session = boto3.session.Session()
        lambda_client = session.client('lambda')
        
        slave_lambda_checker = os.environ['SLAVE_LAMBDA_CHECKER']
        invoke_response = lambda_client.invoke(FunctionName = slave_lambda_checker,
                                           #InvocationType='Event',
                                           #InvocationType="RequestResponse",
                                           Payload = data)
    
        response_object = json.loads(json.loads(invoke_response['Payload'].read().decode())['body'])
        self.queue_outputs.put(response_object)
        self.queue_inputs.task_done()


    def response_maker(self, succesfull = False, exception_str = str):
        if not succesfull:
            self.final_response['body'] = json.dumps(exception_str)
            return self.final_response
        elif self.found_archives: 
            self.final_response['body'] = json.dumps(self.found_archives)
            return self.final_response
        else:
            self.final_response['body'] = json.dumps(self.final_response['body'])
            return self.final_response

         
         
def get_error_line():
    """:
    Funcion que busca la linea del ultimo error, en caso de usarse en cadena (como un except que recive el raise de
    una funcion) se vuelve medianamente innecesario, ya que señalaría a la evocación de la función que falló, no a 
    la linea interna de la funcion que falló
    :returns: La funcion regresa un string con la linea donde hubo la excepcion, sea por error o un raise
    """
    return str(sys.exc_info()[-1].tb_lineno)
    
    
def get_especific_error_line():
    _, _, exc_tb = sys.exc_info()
    return str(traceback.extract_tb(exc_tb)[-1][1])
