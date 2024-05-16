""":
=============================================================

Nombre: lbd-dev-etl-fincor-pucs
Tipo: Lambda AWS
Autor: Jeisson David Botache Yela
Tecnología - Precia

Ultima modificación: 07/07/2022

Para el proyecto delta de finanzas corporativas se implementa una etl que se activa cuando
se carga un archivo nuevo (.xls .xlsx) a la carpeta s3-dev-datalake-sources/corporate_finances/pucs/client/.
Este etl toma el archivo nuevo, lo lee con la librería pandas y devuelve archivo json  con 
la data aligerada a la carpeta original. Esto resuelve dos problemas:
la plataforma estaba tardando demasiado al hacer lectura del archivo excel multiples veces
El archivo json pesa mucho menos que el excel original
# TODO: incluir variables de entorno

Requerimientos:
capas:
capa-precia-utils v.01
capa-pandas-data-transfer v.
capa-openpyxl-s3fs-xlrd-fsspec v.01

Variables de entorno:
no utiliza, el nombre del bucket lo saca del event
=============================================================
"""

import boto3
from io import BytesIO
import json
import logging
import sys

import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

failed_init = False
try:
    logger.info('[__INIT__] Inicializando Lambda ...')
    s3_client = boto3.client('s3')

except Exception as e:
    failed_init = True
    logger.error(f"[__init__] error en inicializacion en la linea, motivo: {str(e)}")

def lambda_handler(event, context):
    logger.warning(f'event de entrada: {str(event)}')
    if failed_init:
        return {'statusCode': 500,'body': json.dumps('Error al inicializar lambda')}
        
    try:
        bucket_name, key_path, tmp_file_path = process_event(event)
        s3_get_file(bucket_name, key_path, tmp_file_path)
        dataframe_from_excel = read_excel(tmp_file_path)
        dataframe_from_excel = json_upload(dataframe_from_excel, bucket_name, key_path)
    except Exception as e:
        return {'statusCode': 500,'body': json.dumps('[lambda_handler] Error en la ejecucion '+ str(e))}
    #TODO implement
    return {'statusCode': 200,'body': json.dumps('Archivo de excel leido y copia en json exitosamente guardada')} # español


def process_event(event):
    try:
        logger.info("[process_event] localizacion de valores clave en event...")
        bucket_name = event['Records'][0]['s3']['bucket']['name'] 
        key_path = event['Records'][0]['s3']['object']['key'] 
        key_path = key_path.replace('+',' ') # Los espacios vacios vienen de s3 como '+'
        tmp_file_path = '/tmp/' + key_path.split('/')[-1]
        logger.info(f'[process_event] Valores encontrados: Bucket_name: {bucket_name}, key_path: {key_path}, tmp_file_path: {tmp_file_path}') 
        return bucket_name, key_path, tmp_file_path
    except Exception as e:
        logger.error(f"[process_event] error en event_processor en la linea {get_error_line()}, motivo: "+str(e))
        raise TypeError("Fallo en la recoleccion de valores clave de event")
    
    
def s3_get_file(bucket_name, key_path, tmp_file_path):
    try:
        logger.info(f'[s3_get_file] Descargando archivo {key_path} desde s3...')
        s3_client.download_file(bucket_name, key_path, tmp_file_path)
        logger.info(f'[s3_get_file] Descarga exitosa.')
    except Exception as e:
        logger.error(f"[s3_get_file] error en s3_get_file en la linea {get_error_line()}, motivo: "+str(e))
        raise TypeError('Fallo la descarga del archivo ' + key_path + ' desde S3')


def read_excel(tmp_file_path):
    try:
        logger.info(f'[read_excel] Transformando el archivo {tmp_file_path[5:]} a dataframe...')
        if tmp_file_path.endswith('xlsx'):
            return pd.read_excel(tmp_file_path, header=None, engine='openpyxl') # TODO: hace la misma concardenacion en la linea  52
        elif tmp_file_path.endswith('xls'):
            return pd.read_excel(tmp_file_path, header=None, engine='xlrd')
        else:
            logger.info('[read_excel] El archivo no era xls o xlsx')
            raise TypeError(f'[read_excel] El archivo {tmp_file_path[5:]} no era xls o xlsx')
        logger.info('[read_excel] Archivo excel transoformado a dataframe exitosamente.')
        
    except Exception as e:
        logger.error(f"[read_excel] error reading excel to dataframe {get_error_line()}, motivo: "+str(e))
        raise TypeError('Fallo la transformacion archivo ' + tmp_file_path[5:] + ' a dataframe')
        
        
def json_upload(dataframe_from_excel, bucket_name, key_path):
    try:
        logger.info('[json_upload] Transformando dataframe a JSON ...')
        dataframe_from_excel.columns = [str(item) for item in dataframe_from_excel.columns]
        df_full_excel_json = dataframe_from_excel.to_json(index=False, orient="table")
        df_json_bytes = BytesIO(bytes(df_full_excel_json,'UTF-8'))
        logger.info('[json_upload] Dataframe transformado a JSON exitosamente,')
        logger.info('[json_upload] Subiendo JSON a bucket S3...')
        key_path = '.'.join(key_path.split('.')[:-1])
        s3_client.upload_fileobj(df_json_bytes, bucket_name, key_path+'.json')
        logger.info('[json_upload] JSON  subido al bucket S3 exitosamente.')
    except Exception as e:
        logger.error("[json_upload] Error al subir el json a s3 {get_error_line()}, motivo: "+str(e))
        raise TypeError('No se subio al bucket S3 la version JSON del archivo excel.')


def get_error_line():
    return str(sys.exc_info()[-1].tb_lineno)