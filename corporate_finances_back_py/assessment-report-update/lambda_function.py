import os
import datetime
import pandas as pd
import json
import io
import boto3
import base64
import sys
import logging
import xlsxwriter
from vars import queries, pyg_order, cash_flow_order
from decorators import handler_wrapper, timing, debugger_wrapper
from utils import get_secret, connect_to_db
from sqlalchemy import text

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

            self.assessment_date = event["queryStringParameters"]["date"]
            self.assessment_date_long = datetime.datetime.strptime(self.assessment_date, "%d-%m-%Y").strftime('%Y-%m-%d %H:%M:%S')
            self.nit = event["queryStringParameters"]["nit"]
            self.periodicity = event["queryStringParameters"]["periodicity"]
            self.user = event["queryStringParameters"]["user"]
            self.context_directory = {'wk': 'CAPITAL DE TRABAJO', 'patrimony': 'PATRIMONIO', 'other_projections': 'OTRAS PROYECCIONES'}
            self.pyg_directory_order = {item:order for item, order in zip(pyg_order, range(len(pyg_order)))}
            self.cash_flow_directory_order = {item:order for item, order in zip(cash_flow_order, range(len(cash_flow_order)))}
            self.output = io.BytesIO()
            self.writer = pd.ExcelWriter(self.output, engine='xlsxwriter')
            self.id_assessment = int()
            self.all_tables_dfs = dict()
            self.exit_dataframes = dict()
            self.assets_results = dict()
            self.signed_url = False
            

        except Exception as e:
            logger.error(f'[__init__] Error inicializando objeto lambda, linea: {get_current_error_line()}, motivo: {str(e)}')
            self.failed_init = True

    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.create_conection_to_db()
            self.get_assessment_id()
            self.acquire_model_atributtes()
            self.create_classification_table()
            self.create_pyg_Table()
            self.create_capex_table()
            self.create_cash_flow_table()
            self.create_debt_table()
            for context, sheet_name in self.context_directory.items():
                self.create_modal_window_table(context, sheet_name)
            self.create_excel_file()
            self.save_to_s3()
            self.generate_presigned_url()
            self.create_response()

            
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
        
        
    @handler_wrapper('Obteniendo el id del proceso de valoración', 'Id del proceso de valoración obtenido con exito', 'Error obteniendo el id de assessment', 'Error adquiriendo identificador de assessment')
    def get_assessment_id(self):
        query = f"""SELECT C.ID FROM COMPANY A, ARCHIVE B, ASSESSMENT C WHERE A.ID = B.ID_COMPANY AND B.ID = C.ID_ARCHIVE 
        AND A.NIT = "{self.nit}" AND B.INITIAL_DATE = "{self.assessment_date_long}" AND B.PERIODICITY = '{self.periodicity}' AND C.USER = '{self.user}'"""
        logger.info(f'[get_assessment_id] Query para buscar el id_Assessment:\n{query}')
        rds_data = self.db_connection.execute(text(query))        
        self.id_assessment = rds_data.scalar()
        logger.info(f'[get_assessment_id] Assessment encontrado: {self.id_assessment}')
        
        
    @handler_wrapper('Adquiriendo atributos de modelo del proceso de valoración', 'Atributos modelo adquiridos exitosamente', 'Error adquiriendo atributos del modelo de valoración', 'Error adquiriendo modelos')        
    def acquire_model_atributtes(self):
        for query_name, query in queries.items():
            q = query.format(self.id_assessment)
            logger.info(f'[acquire_model_atributtes] Query para traer información de la tabla {query_name}:\n{q}')
            self.all_tables_dfs[query_name] = pd.read_sql(q, self.db_connection)
            self.all_tables_dfs[query_name].drop(columns=['ID_ASSESSMENT'], errors = 'ignore', inplace = True)


    @handler_wrapper('Emergiendo las información requeridas', 'Información adquirida emergida con exito', 'Error emergiendo información de assessment adquirida', 'Error emergiendo información requerida')
    def create_classification_table(self):
        id_clasification_dict = {row[1]['ID']:row[1]['CLASSIFICATION'] for row in self.all_tables_dfs['raw_classification'].iterrows()}
        
        for row in self.all_tables_dfs['Classification_B'].iterrows():
            self.all_tables_dfs['Classification_A'].loc[self.all_tables_dfs['Classification_A']['ACCOUNT_NUMBER'].str.startswith(row[1]['ACCOUNT_NUMBER']), 'Clasificación'] = id_clasification_dict[row[1]['ID_RAW_CLASSIFICATION']]
            
        self.all_tables_dfs['Classification_A'].fillna('No aplica', inplace=True)
        self.all_tables_dfs['Classification_A'].drop(columns=['ID_ARCHIVE', 'ACCOUNT_ORDER', 'STATUS', 'ORIGINAL_BALANCE', 'CREATED_DATA', 'ID', 'ID_COMPANY', 'INITIAL_DATE', 'PERIODICITY', 'USER', 'ID', 'ASSESSMENT_DATE'], errors = 'ignore', inplace = True)
        self.all_tables_dfs['Classification_A'].rename(columns = {'ACCOUNT_NUMBER':'Código', 'CHECKED_BALANCE': 'Saldo', 'ACCOUNT_NAME':'Nombre original'}, inplace = True)
        self.all_tables_dfs['Classification_A'].to_excel(self.writer, sheet_name='CLASIFICACIÓN', startrow=0 , startcol=0, index=False)
        
        
        
    @handler_wrapper('Creando tabla de pyg', 'Tabla de pyg terminada', 'Error construyendo tabla de pyg', 'Error contruyendo tabla de pyg')
    def create_pyg_Table(self):
        self.all_tables_dfs['pyg_base'] = self.all_tables_dfs['pyg_base'].merge(self.all_tables_dfs['raw_pyg'], left_on = 'ID_RAW_PYG', right_on = 'ID')
        self.all_tables_dfs['pyg_base'].drop(columns=['ID_RAW_PYG', 'ID', 'IS_SUB'], errors = 'ignore', inplace = True)
        self.all_tables_dfs['pyg_base'] = self.all_tables_dfs['pyg_base'].pivot(index='PYG_ITEM_NAME', columns='DATES', values='VALUE')
        self.all_tables_dfs['pyg_base']['order'] = self.all_tables_dfs['pyg_base'].index.map(self.pyg_directory_order)
        self.all_tables_dfs['pyg_base'].sort_values(['order'], inplace = True)
        self.all_tables_dfs['pyg_base'].drop(columns=['order'], errors = 'ignore', inplace = True)
        
        self.exit_dataframes['PYG'] = self.all_tables_dfs['pyg_base']

        self.all_tables_dfs['pyg_projections'] = self.all_tables_dfs['pyg_projections'].merge(self.all_tables_dfs['raw_pyg'], left_on = 'ID_RAW_PYG', right_on = 'ID')
        
        self.all_tables_dfs['pyg_projections_summary'] = self.all_tables_dfs['pyg_projections'][['PYG_ITEM_NAME', 'PROJECTION_TYPE', 'COMMENT', 'ID_DEPENDENCE']].drop_duplicates() #TODO: debo ver cómo junto pyg_projections_summary y pyg_projections con pyg_base en una sola hoja de excel
        pyg_projections_years = self.all_tables_dfs['pyg_projections']['PROJECTED_DATE'].drop_duplicates()

        atributes_df = pd.DataFrame()
        self.all_tables_dfs['pyg_projections'].fillna(0, inplace=True)
        logger.info(f'[mirar aca] antes de for dataframe de summary:\n{self.all_tables_dfs["pyg_projections_summary"].to_string()}\n\nAtributos:{atributes_df.to_string()}')
        for projected_row in self.all_tables_dfs['pyg_projections_summary'].to_dict(orient="records"):
            atributes = self.all_tables_dfs['pyg_projections'].loc[self.all_tables_dfs['pyg_projections']['PYG_ITEM_NAME'] == projected_row['PYG_ITEM_NAME'], 'ATRIBUTE'].tolist()
            row_atributes_df = pd.DataFrame([atributes], columns =pyg_projections_years, index =[0])
            atributes_df = pd.concat([atributes_df, row_atributes_df])

        logger.info(f'[mirar aca] dataframe de summary:\n{self.all_tables_dfs["pyg_projections_summary"].to_string()}\n\nAtributos:{atributes_df.to_string()}')
        self.all_tables_dfs['pyg_projections_summary'] = self.all_tables_dfs['pyg_projections_summary'].loc[self.all_tables_dfs['pyg_projections_summary']['PROJECTION_TYPE']!='sum']
        self.all_tables_dfs['pyg_projections_summary'].rename(columns = {'PYG_ITEM_NAME':'Linea', 'PROJECTION_TYPE': 'Tipo de proyeccion', 'COMMENT':'Comentario'}, inplace = True)
        self.all_tables_dfs['pyg_projections_summary'] = self.all_tables_dfs['pyg_projections_summary'].merge(self.all_tables_dfs['raw_pyg'], left_on = 'ID_DEPENDENCE', right_on = 'ID')
        
        self.all_tables_dfs['pyg_projections_summary']['order'] = self.all_tables_dfs['pyg_projections_summary']['Linea'].map(self.pyg_directory_order)
        self.all_tables_dfs['pyg_projections_summary'].sort_values(['order'], inplace = True)

        self.all_tables_dfs['pyg_projections_summary'].drop(columns=['ID_DEPENDENCE', 'ID', 'IS_SUB','order'], errors = 'ignore', inplace = True)
        self.all_tables_dfs['pyg_projections_summary'].rename(columns = {'PYG_ITEM_NAME':'Cuennta dependiente'}, inplace = True)
        self.all_tables_dfs['pyg_projections_summary'].to_excel(self.writer, sheet_name='PROYECCIONES DE PYG', startrow=0 , startcol=0, index=False)
        atributes_df.to_excel(self.writer, sheet_name='PROYECCIONES DE PYG', startrow=0 , startcol=len(self.all_tables_dfs['pyg_projections_summary'].columns), index=False)




    @handler_wrapper('Creando tabla de capex', 'Tabla de capex terminada', 'Error creando tabla de capex', 'Error creando tabla de capex')
    def create_capex_table(self):
        assets_properties_df = pd.DataFrame()
        assets_projections_df = pd.DataFrame()
        
        items_groups = self.all_tables_dfs["fixed_assets"]["ID_ITEMS_GROUP"].unique()
        current_row_writing = 0
        for group in items_groups:
            logger.info(f'[mira aca] este es el group que busco: {group}')
            group_df = self.all_tables_dfs['fixed_assets'].loc[self.all_tables_dfs['fixed_assets']['ID_ITEMS_GROUP'] == group]
            used_accounts = group_df[['ASSET_ACCOUNT', 'ACUMULATED_ACCOUNT', 'PERIOD_ACCOUNT']].iloc[0]
            logger.info(f'\n{used_accounts}')
            
            original_values = group_df[['ASSET_ORIGINAL_VALUE', 'ACUMULATED_ORIGINAL_VALUE', 'PERIOD_ORIGINAL_VALUE']].iloc[0]
            logger.info(f'\n{original_values}')
            this_group_properties_df = pd.DataFrame(list(zip(used_accounts, original_values)), columns =['Numero de cuenta', 'Saldo original'])
            
            group_df.drop(columns=['ID_ITEMS_GROUP', 'PROJECTION_TYPE', 'ASSET_ACCOUNT', 'ACUMULATED_ACCOUNT', 'PERIOD_ACCOUNT', 'ASSET_ORIGINAL_VALUE', 'ACUMULATED_ORIGINAL_VALUE' ,'PERIOD_ORIGINAL_VALUE' ,'PROJECTED_YEARS'], errors = 'ignore', inplace = True)
            
            dates = group_df.pop('PROJECTED_DATE')
            group_df.rename(columns = {'ASSET_VALUE':'Activo Bruto', 'ACUMULATED_VALUE': 'Depreciación/Amortizacion acumulada', 'PERIOD_VALUE':'Depreciación/Amortizacion del periodo', 'EXISTING_ASSET_VALUE': 'Activo Neto Existente'}, inplace = True)
            group_df = group_df.T
            group_df.columns = dates
            logger.info(f'[create_capex_table] Resultado de grupo:\n{group_df.to_string()}\n\nResultado de propiedades:\n{this_group_properties_df.to_string()}')
            this_group_properties_df.to_excel(self.writer, sheet_name='ACTIVOS DE DEPRECIACIÓN', startrow=current_row_writing , startcol = 0, index=False)
            group_df.to_excel(self.writer, sheet_name='ACTIVOS DE DEPRECIACIÓN', startrow=current_row_writing , startcol = 4, index=True)

            current_row_writing = current_row_writing + 7
        
        self.all_tables_dfs["capex"].rename(columns = {'USED_ACCOUNT_NAME':'Cuenta utilizada para calculo', 'PERIODS': 'Años de depreciación', 'METHOD':'Metodo de proyección'}, inplace = True)
        self.all_tables_dfs["capex"].to_excel(self.writer, sheet_name='CAPEX', startrow=0 , startcol = 0, index=False)

        capex_dep_dates = self.all_tables_dfs["capex_dep"].pop('CALCULATED_DATE')
        self.all_tables_dfs["capex_dep"].rename(columns = {'MANUAL_PERCENTAGE':'Procentaje manual', 'CAPEX_SUMMARY': 'CAPEX', 'CAPEX_ACUMULATED':'Depreciación de capex'}, inplace = True)
        self.all_tables_dfs["capex_dep"] = self.all_tables_dfs["capex_dep"].T
        self.all_tables_dfs["capex_dep"].columns = capex_dep_dates
        self.all_tables_dfs["capex_dep"].to_excel(self.writer, sheet_name='CAPEX', startrow=4 , startcol=0)
        
        logger.info(f'[create_capex_table] Salida de nuevo capex:\nPropiedades\n{self.all_tables_dfs["capex"]}\n\nResultados:{self.all_tables_dfs["capex_dep"].to_string()}')
        

    @handler_wrapper('Creando tabla final de flujo de caja', 'Tabla de flujo de caja creada con exito', 'Error creando tabla de flujo de caja', 'Error creando tabla de flujo de caja')
    def create_cash_flow_table(self):
        self.all_tables_dfs["cash_flow"].loc[self.all_tables_dfs["cash_flow"]['CASH_FLOW_ITEM_NAME'] == 'Check', 'VALUE']= 'Sí'
        self.all_tables_dfs["cash_flow"].rename(columns = {'SUMMARY_DATE':'Fecha', 'CASH_FLOW_ITEM_NAME': 'Linea de flujo de caja', 'CAPEX_ACUMULATED':'Depreciación de capex'}, inplace = True)
        self.all_tables_dfs["cash_flow"] = self.all_tables_dfs["cash_flow"].pivot(index='Linea de flujo de caja', columns='Fecha', values='VALUE')
        self.all_tables_dfs['cash_flow']['order'] = self.all_tables_dfs['cash_flow'].index.map(self.cash_flow_directory_order)
        self.all_tables_dfs['cash_flow'].sort_values(['order'], inplace = True)
        self.all_tables_dfs['cash_flow'].drop(columns=['order'], errors = 'ignore', inplace = True)
        logger.info(f'[create_cash_flow_table] Salida de flujo de caja:\n{self.all_tables_dfs["cash_flow"].to_string()}')
        self.exit_dataframes['FLUJO DE CAJA'] = self.all_tables_dfs["cash_flow"]
        

    @handler_wrapper('Creando tabla de deuda', 'Tabla de deuda creada con exito', 'Error creando tabla de deuda', 'Error creando tabla de deuda')
    def create_debt_table(self):
        self.all_tables_dfs["debt"].loc[self.all_tables_dfs["debt"]['ACCOUNT_NUMBER'] == '0', 'ACCOUNT_NUMBER'] = 'Deuda futura'
        self.all_tables_dfs["debt"].drop(columns=['ORIGINAL_VALUE', 'PROJECTION_TYPE', 'START_YEAR', 'ENDING_YEAR', 'DEBT_COMMENT', 'RATE_COMMENT', 'SPREAD_COMMENT'], errors = 'ignore', inplace = True)
        self.all_tables_dfs["debt"].rename(columns = {'ACCOUNT_NUMBER':'Numero de cuenta', 'ALIAS_NAME': 'Nombre de deuda', 'INITIAL_BALANCE':'Saldo inicial', 'AMORTIZATION':'Amortizacion', 'INTEREST_VALUE':'Valor de interes', 'ENDING_BALANCE_VARIATION':'Variación de saldo final', 'RATE_ATRIBUTE': 'Tasa de rates', 'SPREAD_ATRIBUTE':'Tasa de spreads'}, inplace = True)
        account_alias_pairs = self.all_tables_dfs["debt"][['Numero de cuenta', 'Nombre de deuda']].drop_duplicates()
        current_debt_df = pd.DataFrame()
        future_debt_df = pd.DataFrame()
        sheet_current_row = 0
        for account_alias_dict in account_alias_pairs.to_dict(orient="records"):
            df_dict = {key:[value] for key, value in account_alias_dict.items()}
            properties = pd.DataFrame(data=df_dict)
            pair_debt_df = self.all_tables_dfs["debt"].loc[(self.all_tables_dfs["debt"]["Numero de cuenta"]==account_alias_dict['Numero de cuenta'])&(self.all_tables_dfs["debt"]["Nombre de deuda"]==account_alias_dict['Nombre de deuda'])]
            pair_debt_df.drop(columns=['Numero de cuenta', 'Nombre de deuda'], errors = 'ignore', inplace = True)
            pair_years = pair_debt_df.pop('YEAR')
            pair_debt_df = pair_debt_df.T
            pair_debt_df.columns = pair_years
            properties.to_excel(self.writer, sheet_name='DEUDA', startrow=sheet_current_row , startcol=0, index = False)
            pair_debt_df.to_excel(self.writer, sheet_name='DEUDA', startrow=sheet_current_row , startcol=5)
            sheet_current_row = len(pair_debt_df.index) + 3

    
    @handler_wrapper('Creando tavla de capital de trabajo', 'Tabla de capital de trabajo creada con exito', 'Error creando tabla de capital de trabajo', 'Error creando tabla de capital de trabajo')
    def create_modal_window_table(self, context, sheet_name):
        contex_found_df = self.all_tables_dfs["modal_window"].loc[self.all_tables_dfs["modal_window"]['CONTEXT_WINDOW'] == context]
        if contex_found_df.empty:
            return
        contex_found_df.rename(columns = {'ACCOUNT_NUMBER':'Numero de cuenta', 'ORIGINAL_VALUE': 'Valor original de cuenta', 'VS_ACCOUNT_NAME':'Depende de la cuenta', 'PROJECTION_TYPE':'Tipo de proyeccion', 'COMMENT':'Comentario', 'ATRIBUTE': 'Atributo'}, inplace = True)
        all_properties_df = contex_found_df[['Numero de cuenta', 'Valor original de cuenta', 'Depende de la cuenta', 'Tipo de proyeccion', 'Comentario']].drop_duplicates()
        logger.info(f'[mira aca] este es el all_properties_df: {all_properties_df.to_string()}')
        sheet_current_row = 0
        for properties_dict in all_properties_df.to_dict(orient="records"):
            df_dict = {key:[value] for key, value in properties_dict.items()}
            properties = pd.DataFrame(data=df_dict).T

            projections_df = contex_found_df.loc[contex_found_df["Numero de cuenta"]==properties_dict['Numero de cuenta']]
            projections_df = projections_df[['PROJECTED_DATE', 'Atributo', 'VALUE']]
            account_years = projections_df.pop('PROJECTED_DATE')
            projections_df = projections_df.T
            projections_df.columns = account_years
            properties.to_excel(self.writer, sheet_name = sheet_name, startrow=sheet_current_row , startcol=0, header = False)
            projections_df.to_excel(self.writer, sheet_name = sheet_name, startrow=sheet_current_row , startcol=3)
            sheet_current_row = sheet_current_row + len(projections_df.index) + 5

    
    @handler_wrapper('Creando archivo de excel', 'Archivo de excel creado con exito', 'Error creando archivo de excel', 'Error creando archivo de excel')
    def create_excel_file(self):
        for sheet, created_table in self.exit_dataframes.items():
            created_table.to_excel(self.writer, sheet_name=sheet, index=True)
            
        self.writer.close()
        self.excel_data = self.output.getvalue()
        
        
    @handler_wrapper('Cargando archivo de excel a s3', 'Archivo de excel cargado con exito', 'Error cargando archivo excel a s3', 'Error cargando archivo')
    def save_to_s3(self):
        self.bucket = os.environ['BUCKET']
        key_folder = os.environ['FOLDER_PATH']
        os.environ['SECRET_DB_REGION']
        s3_client = boto3.client('s3')
        buffer = io.BytesIO(self.excel_data)
        self.file_key = f"{key_folder}{self.nit}-{self.user}-{'-'.join(self.assessment_date_long.split('-')[:3])}.xlsx"
        s3_client.upload_fileobj(buffer, self.bucket, self.file_key)
    
    
    @handler_wrapper('Requiriendo url firmada', 'Url firmada obtenida con exito', 'Error construyendo url firmada', 'Error construyendo url para descarga de excel hacia front')
    def generate_presigned_url(self):
        s3_client = boto3.client('s3')
    
        self.signed_url = s3_client.generate_presigned_url('get_object', 
                                                    Params={'Bucket': self.bucket, 'Key': self.file_key}, 
                                                    ExpiresIn=3600)
        
        
    @handler_wrapper('Construyendo respuesta a front', 'Respuesta construída con exito', 'Error construyendo respuesta a front', 'Error construyendo respuesta')
    def create_response(self):
        if self.signed_url:
            self.partial_response = {'download_url':self.signed_url}
    

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
    