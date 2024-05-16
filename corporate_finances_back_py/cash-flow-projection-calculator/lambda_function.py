""":
capas:
ninguna

variables de entorno:
ninguna

RAM: 25 MB
"""


import json
import logging
import sys
import traceback



from decorators import handler_wrapper, timing


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    lo = lambda_object(event)
    return lo.starter()
    
    
class lambda_object():

    @handler_wrapper('Obteniendo valores clave de event', 'Valores de event obtenidos correctamente',
               'Error al obtener valores event', 'Fallo en la obtencion de valores clave de event')
    def __init__(self, event) -> None:

        logger.warning(f'event que llega a la lambda: {str(event)}')
        event_body_json = event["body"]
        self.received_object = json.loads(event_body_json)
        self.functions_directory = {
            "impositive": self.calculate_impositives,
            "proportion": self.calculate_proportions,
            "fixed": self.calculated_fixeds,
            "constant": self.calculate_constants,
            "input": self.calculate_inputs,
            "zero": self.calculate_zeros
        }

        self.calculated_projection = {'name':self.received_object['name']}
        self.final_response = {'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'}}
        
    
    def starter(self):
        try:
            logger.info(f'[starter] Empezando starter de objeto lambda')
            self.functions_directory[self.received_object['projection']]()
            logger.info(f'[starter] Tareas de starter terminadas con exito')
            return self.response_maker(succesfull = True)
            
        except Exception as e:
            logger.error(f'[starter] Error en el procesamieno del comando de la linea: {get_current_error_line()}, motivo: {e}')
            return self.response_maker(succesfull = False, exception_str = str(e))

    
    @handler_wrapper('Calculando tasa impositiva','Tasa impositiva calculada con exito','Error calculando tasa impositiva','No se pudieron procesar los datos recibidos')
    def calculate_impositives(self):
        proyected_values = []
        for index, vs_value in enumerate(self.received_object["vs_values"]):
            proyected_values.append(vs_value * self.received_object['percentage'][index]/100)
        self.calculated_projection['result'] = proyected_values
        
        
    @handler_wrapper('Calculando proporcion','Proporcion calculada con exito','Error calculando proporcion','No se pudieron procesar los datos recibidos')
    def calculate_proportions(self): 
        proyected_values = [self.received_object["value"] / self.received_object["vs_original_value"] * self.received_object["vs_values"][0]] 
        for index, vs_value in enumerate(self.received_object["vs_values"][1:]):
            proyected_values.append(proyected_values[index] / self.received_object["vs_values"][index] * self.received_object["vs_values"][index+1] )
        self.calculated_projection['result'] = proyected_values
        
        
    
    @handler_wrapper('Calculando tasa fija','Tasa fija calculada con exito','Error calculando tasa fija','No se pudieron procesar los datos recibidos')
    def calculated_fixeds(self):
        proyected_values = [(1+self.received_object["percentage"][0]/100) * self.received_object["value"]]
        for index, p in enumerate(self.received_object["percentage"][1:]):
            proyected_values.append(proyected_values[index] * (1+p/100))
        self.calculated_projection['result'] = proyected_values
        
            
    
    @handler_wrapper('Calculando valor cte','Valor cte calculado con exito','Error calculando valor constante','No se pudieron procesar los datos recibidos')
    def calculate_constants(self):
        self.calculated_projection['result'] = [self.received_object["value"]] * self.received_object["years"]
        
    
    @handler_wrapper('Calculando inputs','Inputs asignados con exito','Error asignando inputs','No se pudieron procesar los datos recibidos')
    def calculate_inputs(self):
        self.calculated_projection['result'] = self.received_object["inputs"]
    
    @handler_wrapper('Calculando zero','Zero asignado con exito','Error asignando zeros','No se pudieron procesar los datos recibidos')    
    def calculate_zeros(self):
        self.calculated_projection['result'] = [0] * self.received_object["years"]
        

        
    def response_maker(self, succesfull = False, exception_str = str):
        if not succesfull:
            self.final_response['body'] = json.dumps(exception_str)
            return self.final_response
        self.final_response['body'] = json.dumps(self.calculated_projection)
        return self.final_response


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)


def get_especific_error_line():
    _, _, exc_tb = sys.exc_info()
    return str(traceback.extract_tb(exc_tb)[-1][1])
    
