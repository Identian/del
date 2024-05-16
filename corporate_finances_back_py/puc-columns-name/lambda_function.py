""":
=============================================================

Nombre: lbd-dev-layer-fincor-puc-columns-name-get
Tipo: Lambda AWS
Autor: Jeisson David Botache Yela
Tecnología - Precia

Ultima modificación: 11/07/2022

Para el proyecto Delta de finanzas corporativas se construyó estra lambda para 
reducir las manualidades que el analista debe realizar. La lambda se activa por 
medio de apigateway y lo que hace es buscar la version json del excel que el 
analista haya cargado a s3 (lbd_dev_fincorp_etl_pucs es la encargada de convertir
el excel a json) y recorrerlo sin modificarlo en busqueda de las cuentas 1 y 11
(100000 y 110000 en caso de puc de super) para así encontrar en qué columna y fila 
empieza la información de puc.
Si el json del excel no existe aún en s3, esta lambda esperará en intervalos de 
un segundo para buscarla nuevamente. Dependiendo del timeout restante decidirá que 
retornará un except personalizado.

Requerimientos:
capas:
capa-pandas-data-transfer v.02

variables de entorno:
BUCKET_NAME : s3-dev-datalake-sources
PUC_BUCKET_PATH	: corporate_finances/pucs/client/

RAM: 1024 MB
=============================================================
"""
import boto3
import json
import logging
import sys
import time

import pandas as pd
from utils import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)
failed_init = False

try:
    logger.info('[__INIT__] Inicializando Lambda ...')
    bucket_name = os.environ['BUCKET_NAME']
    puc_bucket_path = os.environ['PUC_BUCKET_PATH']
    s3_client = boto3.client('s3') 
    logger.info('[__INIT__] Lambda inicializada exitosamente')
    
except Exception as e:
    failed_init = True
    logger.error(f"[__INIT__] error en inicializacion, motivo: "+str(e))


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
    if failed_init:
        return {'statusCode': 500,'body': json.dumps('Error al inicializar lambda')}
    
    try:    
        filename = process_event(event)
        download_file(bucket_name, puc_bucket_path, filename, context)
        df_full_excel = read_json(filename)
        headers_row, account_column = find_columns_master(df_full_excel)
        preview = preview_maker(df_full_excel, headers_row)
        columns_headers_dict = columns_headers_maker(df_full_excel,headers_row)
        return response_maker(columns_headers_dict, headers_row, account_column, preview, succesfull = True)
    
    except Exception as e:
        error_line_int = get_error_line()
        return response_maker({},0,0,[],succesfull = False, exception = str(e), error_line = error_line_int) 


def process_event(event):
    """:
    Funcion que toma el event que disparó la lambda en busqueda del filename envíado por el front.
    En la transformación del filename se debe anotar: los espacios vacíos del filename se convierten
    en '%20', se debe reemplazar; algunos archivos pueden tener varios puntos en el nombre, por lo
    tanto se debe hacer un split por puntos para eliminar el ultimo y eliminar la extenxión del archivo.
    Se vuelven a unir las partes separadas con puntos y se agrega la extensión json
    :param event: Se puede interpretar como variables base que activan la lambda
    :returns: La funcion debe devolver el filename necesario para la descarga
    """
    try:
        logger.info(str(event))
        logger.info('[process_event] Obteniendo valores clave de event')
        filename = event["pathParameters"]["filename"]
        filename = '.'.join(filename.replace('%20',' ').split('.')[:-1]) + '.json'
        logger.info('[process_event] Valores de event obtenidos correctamente')
        return filename
    except Exception as e:
        logger.error(f"[process_event] Error al obtener valores clave de event, linea: {get_error_line()}, motivo: "+str(e))
        raise AttributeError('Fallo en la obtencion de valores clave de event')


def download_file(bucket_name, puc_bucket_path, filename, context):
    """:
    Funcion que toma el filename para descargar el archivo de mismo nombre al /tmp del lambda;
    importante: una lambda automatica lee el archivo que se sube a s3 y hace una copia a json.
    esta copia puede tardar entre 7 y 13 segundos, por lo tanto, esta lambda debe buscarlo por
    un tiempo determinado, si no lo encuentra se debe evitar el timeout entregando una respuesta
    tres segundos antes de que ocurra
    :bucket_name: variable de entorno indicando el bucket donde se encuentra el archivo
    :puc_bucket_path: variable de entorno indicando el key path donde se encuentra el archivo
    :filename: nombre del archivo a buscar
    :context: variable de entorno indicando el bucket donde se encuentra el archivo
    :returns: La funcion no regresa nada, es un void que sólo descarga el archivo, pero es inevitable
    tener un return ya que el while debe decidir cuando escapar por timeout. deben haber otras formas 
    de hacer esto pero esta me pareció la más diciente
    """
    try:
        logger.info(f'[download_file] Descargando archivo: bucket {bucket_name}, key {puc_bucket_path}, filename {filename}')
        remaining_time = context.get_remaining_time_in_millis()
        got_response = False
        while not got_response and remaining_time>3000:
            remaining_time = context.get_remaining_time_in_millis()
            response = s3_client.list_objects_v2(Bucket=bucket_name,Prefix=puc_bucket_path+filename)
            try:
                a = response['Contents']
                s3_client.download_file(bucket_name, puc_bucket_path+filename,'/tmp/'+filename)
                logger.info(f'[download_file] Descarga exitosa.') 
                return  
                
            except Exception as e:
                logger.info("[download_file] Archivo no encontrado, esperando...")
                time.sleep(1)
                logger.info("[download_file] Buscando nuevamente...")
        logger.error(f"[download_file] El archivo no se encontró y el lambda se irá a timeout con remaining_time= {remaining_time} milisegundos")
        raise AttributeError (f"[download_file] Se evita timeout por ausencia de {filename}") 
    except Exception as e:
        raise AttributeError(f"[download_file] Error fatal al realizar la descarga del archivo {filename}")


def read_json(filename):
    """:
    Funcion que toma el filename para hacer la lectura del archivo json descargado como DataFrame, para mejorar
    algunas funciones y el dibujado en el front se cambian los espacios vacios del excel a string vacío
    :filename: nombre del archivo a buscar
    :returns: La funcion regresa el dataframe del json. ya que el excel no sufrió cambios al 
    reformatearse a json lo nombro df_full_excel
    """
    try:
        logger.info(f'[read_json]Leyendo archivo {filename} a dataframe...')
        df_full_excel = pd.read_json('/tmp/'+filename, orient="table")
        df_full_excel.fillna('', inplace=True)
        logger.info(f'[read_json] {filename} convertido a dataframe excitosamente')
        return df_full_excel
    except Exception as e:
        logger.info(f"[read_json] Error al crear dataframe desde el archivo json {filename}, linea: {get_error_line()}, motivo: {str(e)}")
        raise AttributeError(f"[read_json] Error fatal al realizar la lectura del archivo {filename} a dataframe")


def find_columns_master(df_full_excel):
    """:
    Funcion que toma el dataFrame del excel completo y lo inyecta en la funcion find_columns
    junto a los parametros que desea buscar, en un puc normal se deben buscar los parametros
    '1' y '11', en un puc de superfinanciera se deben buscar los parametros '100000' y 
    '110000'. En caso de que el puc no sea default o super se levanta una excepcion que destruye
    la lambda avisando que no se encontró la informacion requerida. Esto se podría manejar
    de forma diferente si en el front se tuviera una pantalla para escoger manualmente las
    columnas y el row donde empieza el puc
    :df_full_excel: DataFrame con la informacion del excel completa
    :returns: La funcion regresa el row donde empieza el puc y la columna donde se encontraron
    los codigos cuentas
    """

    try:
        logger.warning(f'[mirar aca] \n{df_full_excel.head(5).to_string()}')
        logger.info('[find_columns_master] Buscando cuentas 1 y 11...')
        headers_row, account_column = find_columns(df_full_excel,['1','11'])
        logger.info('[find_columns_master] Se encontraron las propiedades como puc default')
        return headers_row, account_column
    except Exception as e:
        logger.warning('[find_columns_master] Error en la busqueda de cuentas default')

    try:
        logger.info('[find_columns_master] Buscando cuentas 100000 y 110000...')
        headers_row, account_column = find_columns(df_full_excel, ['100000','110000'])
        logger.info('[find_columns_master] Se encontraron las propiedades como puc de superfinanciera')
        return headers_row, account_column
    except Exception as e:
        logger.error('[find_columns_master] Error en la busqueda de cuentas superfinanciera')
        raise AttributeError('[find_columns_master] No se encontraron las cuentas de interes, revisar puc')

    

def find_columns(df_full_excel, search_values):
    """:
    La funcion find_columns toma el dataFrame de un excel completo y lo recorre columna a columna en
    busqueda de las search_values, en caso de encontrarlos, se pregunta si la distancia entre estos
    es solo una posicion, es decir si los search_values están uno tras otro. una vez encontrados se 
    devuelve a find_columns_master la columna y la fila donde se encontraron. En los PUCs que se 
    conceptualizaron para el desarrollo del proyecto se tomó en cuenta esta asunción. La plataforma
    tiene entonces la limitación de que el puc debe poseer las cuentas '1' y  '11' o las de superfinanciera
    :df_full_excel: DataFrame con la informacion del excel completa
    :search_values: Listado con las cuentas a buscar
    :returns: La funcion regresa el row donde empieza el puc y la columna donde se encontraron
    los codigos cuentas, en caso de no encontrarse devuelve una excepción que manejará find_columns_master
    """
    try:
        for column in df_full_excel.columns:
            logger.warning(f'[mirar aca] {column}')
            columnListing = df_full_excel[column].iloc[:].astype(str).str.strip().values.tolist()
            #En caso de debug colocar acá un log de qué hay en column y qué hay en column listing
            #logger.info(f'[find_columns] Buscando los datos {str(search_values)} en la columna {column} con data: {str(columnListing)} ')
            if search_values[0] in columnListing and search_values[1] in columnListing:
                index1 = columnListing.index(search_values[0])
                index11 = columnListing.index(search_values[1])
                if index11 - index1 == 1:
                    found_row = True
                    headers_row = index1
                    account_column = column
                    return headers_row , account_column
                    
        raise AttributeError(f'[find_columns] No se encontraron las cuentas {str(search_values)}')
    except Exception as e:
        logger.error(f'[find_columns] Error interno al buscar las columnas de puc, linea: {get_error_line()}, motivo: {str(e)}')
        raise AttributeError('[find_columns] Error al buscar valores clave para encontrar columna de cuentas')


def preview_maker(df_full_excel, headers_row):
    """:
    Funcion que toma el dataFrame del excel completo y lo recorre fila a fila para generar un array de lineas json
    para enviarle al front una preview de las lineas superiores del puc. Se hizo de esta manera ya que lso PUCs no
    tienen una forma universal, y por lo tanto no se sabe cuantas columnas debera recibir y dibujar el front. 
    Tambien se probó la idea de envíar un array de arrays de pero al convertirlos a json dejaban de ser legibles 
    para Angular. Al cambiar rows_to_display puedes cambiar la cantidad de filas a llevar al front
    :df_full_excel: DataFrame con la informacion del excel completa
    :headers_row: numero de fila donde se encontró que empieza el puc
    :returns: La funcion regresa un array de jsons con la cantidad de filas deseadas y a partir de donde empieza
    la informacion relevante del puc
    """
    try:
        logger.info(f'[preview_maker] Construccion de preview...')
        rows_to_display = 20
        df_full_excel = df_full_excel.reset_index(drop = True)
        outerList = []

        for _, row in df_full_excel[(headers_row-1):].head(rows_to_display).iterrows():
            innerList = row.astype(str).values.tolist()        
            outerList.append(json.dumps(innerList))
        return outerList
    
    except Exception as e:
        logger.error(f'[preview_maker] Error al construir el preview, linea: {get_error_line()}, motivo: {str(e)}')
        raise AttributeError('[preview_maker] Error al construir el preview')


def columns_headers_maker(df_full_excel, headers_row):
    """:
    Esta funcion crea un objeto clave valor con los valores de los encabezados (la linea inmediatamente arriba 
    de donde se encontró la cuenta '1' u '110000') y su index, ya que en algunos pucs hay encabezados con el mismo
    nombre y por lo tanto la depuración debe hacerse por el numero de columna, no con su nombre. En la mayoría de
    pucs las columnas como encabezados celdas vacías y era incomodo escogerlas para hacer la depuración. Para llenar
    estas celdas se construye un array de columnas de excel: A,B..AA,AB,AC... para llenar estas celdas. este
    algoritmo funciona tambien para darle nombre a los encabezado en el raro caso donde hay una fila vacia 
    entre la data del puc y los encabezados.
    :df_full_excel: DataFrame con la informacion del excel completa
    :headers_row: numero de fila donde se encontró que empieza el puc
    :returns: La funcion regresa un objeto clave valor que representa la posicion y el valor del encabezado
    """
    try:
        logger.info(f'[columns_headers_maker] construyendo encabezados de puc...')
        logger.info(f'[columns_headers_maker] creando listado de columnas excel...')
        letters = list('ABCDEFGHIJKLMNOPQRSTUVWXYZ') # TODO: Puede existir que llegue hasta la columna AA o mayor? sí, esto es para generar AA, AB y así hasta ZZ pero ningun puc debería llegar hasta allá
        letters_len = len(letters)
        j = 0
        while len(letters) < len(df_full_excel.columns):
            for i in range(letters_len):
                letters.append(letters[j] + letters[i])
            j = j + 1
        logger.info(f'[columns_headers_maker] obtener los encabezados como listado...')
        columns_headers_list = df_full_excel.iloc[headers_row-1].astype(str).values.flatten().tolist()
        logger.info(f'[columns_headers_maker] reemplazando encabezados vacios...')
        for i, val in enumerate(columns_headers_list):
            if val == "":
                columns_headers_list[i] = "Columna "+letters[i]
        logger.info(f'[columns_headers_maker] creando diccionario index-columna...')
        return dict(zip(range(len(columns_headers_list)),columns_headers_list))
    
    except Exception as e:
        logger.error(f'[columns_headers_maker] Error al construir los encabezados, linea: {get_error_line()}, motivo: {str(e)}')
        raise AttributeError('[columns_headers_maker] Error al construir los encabezados')


def response_maker(columns_headers_dict, headers_row, account_column, preview, succesfull = True, exception = '', error_line = 0):
    """:
    Funcion que construye la respuesta general del lambda, para llegar se activa si todo salió bien en lambda handler
    o si algo salió mal y se disparó el la excepcion general de esta. en el primer caso debo enviar aquí el contenido
    de interés para el front: el clave valor de encabezados, la fila donde empieza la data del puc, la columna donde
    se encontraron los codigos cuenta y el preview del excel. En caso de llegar a este metodo por medio de una excepcion
    se trae un motivo de falla que no será mostrado al analista a menos que lo esté asesorando un desarrollador.
    :columns_headers_dict: objeto clave valor de encabezados
    :headers_row: numero de fila donde se encontró que empieza el puc
    :account_column: numero de columna donde se encontraron los codigos cuenta
    :preview: array de jsons de primeras filas del puc
    :succesfull: determina si el metodo se está ejecutando como excepcion o como salida correcta
    :exception: motivo de error
    :error_line: linea de falla
    :returns: la funcion regresa el objeto alistado que lambda handler rergesará al disparador. status_code dependiente
    si hubieron errores y mensajes acordes o una respuesta positiva con los objetos que el front requiere. En cualquier
    caso se deben agregar tambien los encabezados 'Acces-Control' para que api gateway no corte la comunicacion back-front
    """
    if not succesfull:
        try:
            logger.warning(f'[response_maker] Creando respuesta de error..., error en la linea {error_line}, motivo: {exception}')
            error_response  = {'statusCode': 500,
            'headers': {'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*'}, 
            'body' : json.dumps(f'error de ejecucion, motivo: {exception}')} # TODO: Esto es de valor para el front? sí jeje, al menos mientras salimos a prod
            return error_response
        except Exception as e:
            logger.error(f'[response_maker] Error al construir la respuesta de error, linea: {get_error_line()}, motivo: {str(e)}')
            return {'statusCode': 500,
            'headers': {'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*'}, 
            'body' : json.dumps(f'[admin] error al crear respuesta')}
            
    try:
        logger.info('[response_maker] creando respuesta ')
        ok_response  = {'statusCode': 200,
        'headers': {'Access-Control-Allow-Headers': '*',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': '*'}}
        
        body = {'status_code':200}
        body["columns"] = columns_headers_dict
        body["row"] = headers_row
        body["accounts"] = int(account_column)
        body["file_preview"] = preview
        
        ok_response["body"] = json.dumps(body)
        return ok_response
    
    except Exception as e:
        logger.error(f'[response_maker] Error al construir la respuesta de aceptado, linea: {get_error_line}, motivo: {str(e)}')
        return {'statusCode': 500,
        'headers': {'Access-Control-Allow-Headers': '*',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': '*'}, 
        'body' : json.dumps(f'[admin] error al crear respuesta')}
        
         
def get_error_line():
    """:
    Funcion que busca la linea del ultimo error, en caso de usarse en cadena (como un except que recive el raise de
    una funcion) se vuelve medianamente innecesario, ya que señalaría a la evocación de la función que falló, no a 
    la linea interna de la funcion que falló
    :returns: La funcion regresa un string con la linea donde hubo la excepcion, sea por error o un raise
    """
    return str(sys.exc_info()[-1].tb_lineno)