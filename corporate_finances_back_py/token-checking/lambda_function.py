""":
variables de entorno:
SECRET_AUTENTICATION_INFO : finanzas/valoracionempresas/autenticacion
SECRET_REGION : us-east-1

capas:
capa-requests-cryptography-pyjwt-3-9

ram:
1024MB

"""


import jwt
import requests
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.backends import default_backend
import logging
import sys
import os
import json
from utils import get_secret

from decorators import handler_wrapper, timing, debugger_wrapper

#logging.basicConfig() #En lambdas borra este

logger = logging.getLogger()
logger.setLevel(logging.INFO)
#raise Exception('fallida esperada')

def lambda_handler(event,context):
    lo = lambda_object(event)
    return lo.starter()
    
    
class lambda_object:
    def __init__(self, event):
        try:
            self.failed_init = False
            #self.final_response = {"isAuthorized": "true","context": {"exampleKey": "exampleValue"}}
            self.final_response = {"principalId": "user",
                                      "policyDocument": {
                                        "Version": "2012-10-17",
                                        "Statement": [
                                          {
                                            "Action": "execute-api:Invoke",
                                            "Effect": "Deny", #Allow  #Deny
                                            "Resource": str()}]}}

            
            self.detailed_raise = ''
            self.partial_response = {}
            
            logger.warning(f'event de entrada: {str(event)}')
            self.token = event['authorizationToken']
            #self.company_table = os.environ['COMPANY_TABLE']
            
            self.final_response['policyDocument']['Statement'][0]['Resource'] = event['methodArn']


        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.get_secret_information()
            self.check_token()

            return self.response_maker(succesfull_run = True)
        except Exception as e:
            logger.error(f'[starter] Hubieron problemas en el comando de la linea: {get_current_error_line()}')
            return self.response_maker(succesfull_run = False, error_str = (str(e)))
      
    
    @handler_wrapper('Obteniendo secretos', 'Secretos obtenidos con exito', 'Error obteniendo secretos', 'Error descifrando información empresarial')
    def get_secret_information(self):
        secret_autentication_info = os.environ['SECRET_AUTENTICATION_INFO']
        secret_region = os.environ['SECRET_REGION']
        
        autenticacion_info = get_secret(secret_region, secret_autentication_info)
        #logger.info(f'[mira aca] {autenticacion_info}')
        self.ms_tenant = autenticacion_info['ms_tenant']
        self.client_id = autenticacion_info['client_id']

    
    @handler_wrapper('Chequeando token', 'Token chequeado, generando respuesta', 'Error chequeando token', 'Error chequeando token')
    def check_token(self):
        PEMSTART = "-----BEGIN CERTIFICATE-----\n"
        PEMEND = "\n-----END CERTIFICATE-----\n"
        
        jwt_token = self.token
        #ms_tenant = "caa1dfbf-34d5-4061-9cdf-0ceaa516bf03"
        #client_id = "dcd76a6c-30fe-4390-bf3c-dfefd29d304c"
        
        jwks_uri = f"https://login.microsoftonline.com/{self.ms_tenant}/discovery/v2.0/keys"
        
        #issuer = f'https://login.microsoftonline.com/{ms_tenant}/v2.0'
        issuer = f'https://sts.windows.net/{self.ms_tenant}/'
        
        uri_response = requests.get(jwks_uri).json()
        keys = uri_response.get("keys", False)
    
        if keys:
            kid = jwt.get_unverified_header(jwt_token).get("kid")
            for key in keys:
                if kid == key["kid"]:
                    cert = PEMSTART + key["x5c"][0] + PEMEND
                    cert_obj = load_pem_x509_certificate(cert.encode(), default_backend())
                    public_key = cert_obj.public_key()
                    decoded = jwt.decode(jwt_token, public_key, algorithms=["RS256"], audience=self.client_id, issuer=issuer) #aca cambie el tenant por client id
                    #logger.warning(str(decoded))
                    logger.warning(f"[check_token] El usuario {decoded['unique_name']} se autenticó correctamente")
                    self.final_response['policyDocument']['Statement'][0]['Effect'] = 'Allow'
                    self.final_response['context'] = {'stringKey':decoded['unique_name']}
                    break
        
            
    @debugger_wrapper('Error construyendo respuesta final', 'Error construyendo respuesta')
    def response_maker(self, succesfull_run = False, error_str = str()):
        return self.final_response


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
    