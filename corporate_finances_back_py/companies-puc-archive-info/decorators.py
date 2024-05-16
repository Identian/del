#version 2022 - 12 - 06
import logging
import sys
import traceback
from functools import wraps
from time import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler_wrapper(start, satisfactory_log, error_log, raise_error):
    #esto se ejecuta en la inicializacion de primero
    def decorator(func):
        #print(message1)
        #lo que yo ponga acá se ejecuta antes de hacer el llamado a la funcion
        def wrapper(*args, **kwargs):
            #esto sí se ejecuta solo cuando llama a la funcion decorada
            #print(message2)
            try:
                logger.info(f'[{func.__name__}] {start}...')
                resultado = func(*args, **kwargs)
                logger.info(f'[{func.__name__}] {satisfactory_log}')
                return resultado
            except Exception as e:
                logger.error(f"[{func.__name__}] {error_log}, linea: {get_especific_error_line(func.__name__)}, motivo: {str(e)}")
                raise Exception(f"{raise_error}")
                
        return wrapper
    return decorator
    
    
def get_especific_error_line(func_name):
    _, _, exc_tb = sys.exc_info()
    for trace in traceback.extract_tb(exc_tb):
        if func_name in trace:
            return str(trace[1])
            
def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        taken_time_message =  '[timing] La funcion {}, toma: {:.2f} sec en ejecutarse'.format(f.__name__, te-ts)
        logger.info(taken_time_message)
        return result
    return wrap
