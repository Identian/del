from flask import Flask, request
from flask_cors import CORS, cross_origin
import os
import sys
import logging

cwd = os.getcwd()
app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


logging.basicConfig(filename='app.log',level=logging.DEBUG)
handler = logging.FileHandler('app.log')  # Log to a file
app.logger.addHandler(handler)
####################################
@app.route('/assessment/pyg/projections-calculator', methods = ['POST'])
@cross_origin()
def pyg_projections_calculator():
    sys.path.insert(1, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\pyg_projections_calculator')
    from pyg_projections_calculator.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/pucs/depurated/files', methods = ['POST'])
@cross_origin()
def multi_puc_depurate():
    sys.path.insert(2, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\multi_puc_depurate')
    from multi_puc_depurate.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/assessment/assessment/database', methods = ['POST'])
@cross_origin()
def assessment_to_db():
    sys.path.insert(3, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\assessment_to_db')
    from assessment_to_db.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/orchestrator/status/<path:id_assessment>', methods = ['GET']) #TODO: Error de importaciones
@cross_origin()
def status_seeker(id_assessment):
    sys.path.insert(4, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\status_seeker')
    from status_seeker.lambda_function import lambda_handler
    event = {"pathParameters":{"id_assessment": id_assessment}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/recovery/assessment/<path:id_assessment>', methods = ['GET'])  #OK
@cross_origin()
def assessment_retrieve(id_assessment):
    sys.path.insert(5, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\assessment_retrieve')
    from assessment_retrieve.lambda_function import lambda_handler
    event = {"pathParameters":{"id_assessment": id_assessment}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/assessment/cash-flow/databases/basic-modal-windows', methods = ['POST'])
@cross_origin()
def modal_window_saver():
    sys.path.insert(6, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\modal_window_saver')
    from modal_window_saver.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/assessment/pyg/implicit_growings', methods = ['POST'])
@cross_origin()
def implicit_growing():
    sys.path.insert(7, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\implicit_growing')
    from implicit_growing.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/assessment/classification/<path:id_assessment>', methods = ['GET'])        #TODO: Error de importaciones
@cross_origin()
def initial_account_classification(id_assessment):
    sys.path.insert(8, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\initial_account_classification')
    from initial_account_classification.lambda_function import lambda_handler
    event = {"pathParameters":{"id_assessment": id_assessment}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/pucs/depurated/full-checks', methods = ['POST'])
@cross_origin()
def puc_checks():
    sys.path.insert(9, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\puc_checks')
    from puc_checks.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/recovery/capex/calculation/<path:id_assessment>', methods = ['GET'])  #TODO: Error de importaciones
@cross_origin()
def capex_calculation_retrieve(id_assessment):
    sys.path.insert(10, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\capex_calculation_retrieve')
    from capex_calculation_retrieve.lambda_function import lambda_handler
    event = {"pathParameters":{"id_assessment": id_assessment}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/report', methods = ['GET'])        #TODO: Error de importaciones
@cross_origin()
def assessment_report_update():
    sys.path.insert(11, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\assessment_report_update')
    from assessment_report_update.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/assessment/capex/assets/<path:id_assessment>', methods = ['GET'])          #TODO: Error de importaciones
@cross_origin()
def capex_depreciation_items(id_assessment):
    sys.path.insert(12, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\capex_depreciation_items')
    from capex_depreciation_items.lambda_function import lambda_handler
    event = {"pathParameters":{"id_assessment": id_assessment}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/assessment/capex/assets', methods = ['POST'])
@cross_origin()
def capex_depreciation_items_to_db():
    sys.path.insert(13, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\capex_depreciation_items_to_db')
    from capex_depreciation_items_to_db.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/assessment/capex/new_capex', methods = ['POST'])
@cross_origin()
def capex_calculation_to_db():
    sys.path.insert(14, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\capex_calculation_to_db')
    from capex_calculation_to_db.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/assessment/classification/purged_results', methods = ['POST'])
@cross_origin()
def assessment_summary_builder():
    sys.path.insert(15, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\assessment_summary_builder')
    from assessment_summary_builder.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/companies/<path:nit>', methods = ['GET'])                          #TODO: Error de importaciones
@cross_origin()
def companies_exists(nit):
    sys.path.insert(16, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\companies_exists')
    from companies_exists.lambda_function import lambda_handler
    event = {"pathParameters":{"nit": nit}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/recovery/cash-flow/<path:id_assessment>', methods = ['GET'])            #TODO: Error de importaciones
@cross_origin()
def cash_flow_retrieve(id_assessment):
    sys.path.insert(17, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\cash_flow_retrieve')
    from cash_flow_retrieve.lambda_function import lambda_handler
    event = {"pathParameters":{"id_assessment": id_assessment}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/companies', methods = ['GET', 'POST'])          #TODO: revisar los casos de los dos metodos, va a tocar hacer un  if request.method == 'POST':...
@cross_origin()
def company_assessments_info():
    sys.path.insert(19, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\company_assessments_info')
    from company_assessments_info.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/recovery/capex/assets/<path:id_assessment>', methods = ['GET'])            #TODO: Error de importaciones
@cross_origin()
def capex_depreciation_items_retrieve(id_assessment):
    sys.path.insert(20, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\capex_depreciation_items_retrieve')
    from capex_depreciation_items_retrieve.lambda_function import lambda_handler
    event = {"pathParameters":{"id_assessment": id_assessment}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/recovery/classification/<path:id_assessment>', methods = ['GET'])         #OK
@cross_origin()
def classifications_retrieve(id_assessment):
    sys.path.insert(21, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\classifications_retrieve')
    from classifications_retrieve.lambda_function import lambda_handler
    event = {"pathParameters":{"id_assessment": id_assessment}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/pucs/client/data/<path:nit>', methods = ['GET'])       #Candidato a depreciacion pero event ok
@cross_origin()
def companies_puc_archive_info(nit):
    sys.path.insert(22, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\companies_puc_archive_info')
    from companies_puc_archive_info.lambda_function import lambda_handler
    event = {"pathParameters":{"nit": nit}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/assessment/classification/user-classified-properties', methods = ['POST'])
@cross_origin()
def multi_account_classification():
    sys.path.insert(23, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\multi_account_classification')
    from multi_account_classification.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/assessment/cash-flow/databases/finance-debt', methods = ['POST'])
@cross_origin()
def debt_saver():
    sys.path.insert(24, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\debt_saver')
    from debt_saver.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/recovery/pyg/completed/<path:id_assessment>', methods = ['GET'])  #OK
@cross_origin()
def pyg_retrieve(id_assessment):
    sys.path.insert(25, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\pyg_retrieve')
    from pyg_retrieve.lambda_function import lambda_handler
    event = {"pathParameters":{"id_assessment": id_assessment}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/recovery/cash-flow/modal-window/projection', methods = ['GET'])  #TODO: recrear event a objeto: {"queryStringParameters": {"context": "wk","id_assessment": "2028"}}
@cross_origin()
def modal_projections_retrieve():
    sys.path.insert(26, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\modal_projections_retrieve')
    from modal_projections_retrieve.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/recovery/cash-flow/modal-window/summary', methods = ['GET'])           #TODO: Error de importaciones
@cross_origin()
def modal_summary():
    sys.path.insert(27, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\modal_summary')
    from modal_summary.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/assessment/classification/from-archive-clasifier', methods = ['POST'])
@cross_origin()
def account_classification_from_current():
    sys.path.insert(28, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\account_classification_from_current')
    from account_classification_from_current.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/recurrence', methods = ['POST'])
@cross_origin()
def super_orchestrator():
    sys.path.insert(29, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\super_orchestrator')
    from super_orchestrator.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/assessment/classification/skipped', methods = ['POST'])
@cross_origin()
def skipped_classifications_checker():
    sys.path.insert(30, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\skipped_classifications_checker')
    from skipped_classifications_checker.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/model/catch-all', methods = ['POST'])
@cross_origin()
def full_model_saver():
    sys.path.insert(31, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\full_model_saver')
    from full_model_saver.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/pucs/client/data/columns/<path:filename>', methods = ['GET'])  #este está peligrosky, se supone que a esta area puedo acceder solo después de subir el archivo puc
@cross_origin()
def puc_columns_name(filename):
    sys.path.insert(32, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\puc_columns_name')
    from puc_columns_name.lambda_function import lambda_handler
    event = {"pathParameters":{"filename": filename}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/orchestrator', methods = ['GET'])      #TODO, recrear event a objeto
@cross_origin()
def orchestrator():
    sys.path.insert(33, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\orchestrator')
    from orchestrator.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/recovery/capex/summary/<path:id_assessment>', methods = ['GET'])       #TODO: error de importaciones
@cross_origin()
def capex_summary_retrieve(id_assessment):
    sys.path.insert(34, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\capex_summary_retrieve')
    from capex_summary_retrieve.lambda_function import lambda_handler
    event = {"pathParameters":{"id_assessment": id_assessment}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/assessment/cash-flow/projections_calculator', methods = ['POST'])
@cross_origin()
def cash_flow_projection_calculator():
    sys.path.insert(35, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\cash_flow_projection_calculator')
    from cash_flow_projection_calculator.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/recovery/pyg/projections-only/<path:id_assessment>', methods = ['GET']) #si error está devolviendo 0
@cross_origin()
def pyg_projections_retrieve(id_assessment):
    sys.path.insert(36, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\pyg_projections_retrieve')
    from pyg_projections_retrieve.lambda_function import lambda_handler
    event = {"pathParameters":{"id_assessment": id_assessment}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/pucs/depurated/checks', methods = ['POST'])
@cross_origin()
def checker_to_db():
    sys.path.insert(37, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\checker_to_db')
    from checker_to_db.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/recovery/cash-flow/debt/properties/<path:id_assessment>', methods = ['GET']) #TODO error de importacion
@cross_origin()
def debt_retrieve(id_assessment):
    sys.path.insert(38, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\debt_retrieve')
    from debt_retrieve.lambda_function import lambda_handler
    event = {"pathParameters":{"id_assessment": id_assessment}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/classifications', methods = ['GET'])       #TODO: no sé que le falla, al parecer necesitaba un query en la url
@cross_origin()
def classifications():
    sys.path.insert(39, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\classifications')
    from classifications.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/recovery/cash-flow/debt/amortization/<path:id_assessment>', methods = ['GET']) #TODO: ERROR de imporacion
@cross_origin()
def debt_amortize(id_assessment):
    sys.path.insert(40, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\debt_amortize')
    from debt_amortize.lambda_function import lambda_handler
    event = {"pathParameters":{"id_assessment": id_assessment}}
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route('/assessment/cash-flow/modal-windows/pool', methods = ['GET'])  #TODO: acomodar query a event
@cross_origin()
def cash_flow_modal_pool():
    sys.path.insert(41, r'c:\Users\JeissonBotache\OneDrive - PRECIA Proveedor de precios para valoración S.A\Scripts 2024\0228 swagger to server\cash_flow_modal_pool')
    from cash_flow_modal_pool.lambda_function import lambda_handler
    event = request.query_string.decode()
    response = lambda_handler(event, 'From_Flask')
    return response





###############################################
@app.route("/path/<path:id_assessment>/", methods = ['GET'])
def path_testing(id_assessment):
    #http://127.0.0.1:5000/testing/heloo
    sys.path.insert(1, f'{cwd}\testing')
    from testing.lambda_function import lambda_handler
    event = id_assessment                   #TODO: este event se tiene que mandar como query string parameters
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route("/query", methods = ['GET'])
def query_testing():
    #http://127.0.0.1:5000/testing?heloo
    sys.path.insert(2, f'{cwd}\testing')
    from testing.lambda_function import lambda_handler
    event = request.query_string.decode()   #TODO: este event se tiene que mandar como query string parameters
    response = lambda_handler(event, 'From_Flask')
    return response


@app.route("/posting", methods = ['POST'])
def post_testing():
    #http://127.0.0.1:5000/posting
    sys.path.insert(3, f'{cwd}\testing')
    from testing.lambda_function import lambda_handler
    event = request.get_json()
    response = lambda_handler(event, 'From_Flask')
    return response

