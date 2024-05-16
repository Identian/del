from sqlalchemy import text
from decorators import handler_wrapper, debugger_wrapper
import logging
import datetime
import sys
from models_tables import models_tables
from dataclasses import dataclass, asdict#, field #field es para cuando se debe crear algo en __post_init__
from dataclasses_box import assessment_checked_object, classification_summary_object

logger = logging.getLogger()
logger.setLevel(logging.INFO)


    
class recurrence_class(object):
    @debugger_wrapper('Error en starter de recurrence_class', 'Error en master de recurrencia')
    def full_recurrence_starter(self):
        self.archive_pucs_data = dict()
        self.assessment_checked_records = list()
        self.classification_summary_records = list()
        
        self.assesstment_archive_data()
        self.get_assessment_data()

        self.get_company_id()
        self.acquire_model_atributtes()
        self.calculate_assessment_historic_dates()
        self.upload_historic_assessment_dates()

        if not self.assessment_models['MODEL_USER_CLASSIFICATION']:
            return #Este return me asegura que si no se hallaron modelos, no se va a construir nada de recucrrencia; esta validaciòn la hace el front con otro servicio, colocarlo acá puede ser redundante
        

        self.df_and_upload(self.assessment_models['MODEL_USER_CLASSIFICATION'], 'USER_CLASSIFICATION')
        self.save_assessment_step("CLASSIFICATION")
        if not self.calculate_recurrence: #Si el front pide no hacer recurrencia, no se hace nada más
            return
    
        
        self.calculate_assessment_projection_dates()
        
        self.upload_new_assessment_dates()
        self.new_assessment_summary_builder()
        self.master_process_recurrence_pucs()
        
        self.df_and_upload(self.assessment_checked_records, 'ASSESSMENT_CHECKED')
        
        self.df_and_upload(self.classification_summary_records, 'CLASSIFICATION_SUMMARY')
        
        self.df_and_upload(self.assessment_models['MODEL_USER_CLASSIFICATION'], 'USER_CLASSIFICATION')
        

    @handler_wrapper('Adquiriendo información del puc a valorar', 'Información adquirida con exito', 'Error adquiriendo información del PUC a valorar', 'Error adquiriendo información de puc')
    def assesstment_archive_data(self):
        query = 'SELECT B.ID FROM COMPANY A, ARCHIVE B WHERE A.ID = B.ID_COMPANY AND A.NIT = :nit AND B.INITIAL_DATE = :current_long_date AND B.PERIODICITY = :current_periodicity LIMIT 1'
        logger.info(f'[assesstment_archive_data] Query para obtener datos del puc de valoracion: {query}')
        rds_data = self.db_connection.execute(text(query), {'nit':self.nit, 'current_long_date': self.current_long_date, 'current_periodicity': self.current_periodicity})
        self.this_assessment_archive_id = rds_data.scalar()
    
    
    @handler_wrapper('Buscando existencia del modelo','Chequeo de id_assessment terminado','Error chequeando si existe un id_assessment para este proceso de valoración en particulas','Se encontraron problemas chequeando posible existencia del proceso de valoración')
    def get_assessment_data(self):
        query = 'SELECT * FROM COMPANY A, ARCHIVE B, ASSESSMENT C WHERE A.ID = B.ID_COMPANY AND C.ID_ARCHIVE = B.ID AND A.NIT = :nit AND B.INITIAL_DATE = :current_long_date AND B.PERIODICITY = :current_periodicity AND C.USER = :user LIMIT 1'
        logger.info(f'[get_assessment_data] Query para obtener datos del proceso de valoracion: {query}')
        rds_data = self.db_connection.execute(text(query), {'nit':self.nit, 'current_long_date': self.current_long_date, 'current_periodicity': self.current_periodicity, 'user':self.user})
        try:
            self.assessment_data = [row._asdict() for row in rds_data.fetchall()][0]
            
            self.id_assessment = self.assessment_data['ID']
            logger.info(f'[get_assessment_data] El proceso de valoración fue encontrado y tiene un id_assessment: {self.id_assessment}')
        except Exception as e:
            logger.info(f'[get_assessment_data] El proceso de valoración no existe, {str(e)}')
            today_date = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            query = f'INSERT INTO ASSESSMENT (ID_ARCHIVE, USER, ASSESSMENT_DATE,STATUS) VALUES ({self.this_assessment_archive_id}, "{self.user}", "{today_date}", \"Partial\")'
            logger.info(f'[create_id_assessment] Query de creacion assessment: {query}')
            self.db_connection.execute(text(query))
            self.get_assessment_data()
            
            
    @handler_wrapper('Obteniendo el ID de la empresa', 'ID de la empresa obtenido con exito', 'Error obteniendo ID de la empresa', 'Error obteniendo identificador de empresa')
    def get_company_id(self):
        query = "SELECT ID FROM COMPANY WHERE NIT = :nit LIMIT 1"
        logger.info(f'[get_company_id] Query para obtener el ID de la empresa que se está valorando: {query}')
        rds_data = self.db_connection.execute(text(query), {'nit':self.nit})
        self.id_company = int(rds_data.scalar())

    @handler_wrapper('Adquiriendo atributos de modelo del proceso de valoración', 'Atributos modelo adquiridos exitosamente', 'Error adquiriendo atributos del modelo de valoración', 'Error adquiriendo modelos')        
    def acquire_model_atributtes(self):
        #TODO: Mirar en models_tables.py el directorio ordered_atributes_tables, es posible que estos modelos deban recogerse con un sql organizado por las columnas de fechas
        for table in models_tables:
            query = f"SELECT * FROM {table} WHERE ID_COMPANY = :id_company"
            logger.info(f'[acquire_model_atributtes] Query para el modelo de la tabla {table}:\n{query}')
            rds_data = self.db_connection.execute(text(query), {'id_company': self.id_company})
            self.assessment_models[table] = [row._asdict() for row in rds_data.fetchall()]
            for row in self.assessment_models[table]:
                del row['ID_COMPANY']
            logger.warning(f'[acquire_model_atributtes] Modelo hallado para la tabla {table}:\n{self.assessment_models[table]}')
    
    
    @handler_wrapper('Eligiendo fechas historicas del nuevo proceso de valoración', 'Fechas historicas elegidas con exito', 'Error eligiendo fechas historicas del proceso de valoración', 'Error calculando fechas historicas del nuevo proceso de valoración')
    def calculate_assessment_historic_dates(self):
        #model_historic_dates = int(self.assessment_models['MODEL_ASSESSMENT_YEARS'][0]['HISTORIC_YEARS'])
        query = "SELECT * FROM ARCHIVE WHERE ID_COMPANY = :id_company" #Posiblemente acá deba colocar la periodicidad
        logger.info(f'[calculate_assessment_historic_dates] Query para obtener los datos de archives de la empresa:\n{query}')
        rds_data = self.db_connection.execute(text(query), {'id_company': self.id_company})
        
        company_pucs_data = {row.INITIAL_DATE.strftime('%Y-%m-%d'): row.ID for row in rds_data.fetchall()}
        logger.warning(f'[calculate_assessment_historic_dates] data de pucs hallados para la empresa de nit {self.nit}:\n{company_pucs_data}')
                
        self.new_assessment_historic_archives = [company_pucs_data[date] for date in self.historic_dates_chosen]
        self.historic_dates = sorted(self.historic_dates_chosen)
        logger.info(f'[calculate_assessment_historic_dates] historicos encontrados para recurrencia:\n{self.historic_dates}')


    @handler_wrapper('Organizando los datos construídos a dataframes', 'Dataframes creados con exito', 'Error creando dataframes de guardado', 'Error creando tablas de datos base')
    def upload_historic_assessment_dates(self):
        dates_records = []
        dates_records.extend([{'DATES': datetime.datetime.strptime(str(date), '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'), 'PROPERTY': 'HISTORIC'} for date in self.historic_dates])
        self.df_and_upload(dates_records, 'ASSESSMENT_DATES')


    @handler_wrapper('Calculando fechas de proyección', 'fechas de proyeccion calculadas con exito', 'Error calculando fechas de proyeccion', 'No se pudo calcular fechas de proyección')            
    def calculate_assessment_projection_dates(self):
        model_projection_years = int(self.assessment_models['MODEL_ASSESSMENT_YEARS'][0]['PROJECTION_YEARS'])
        self.projection_dates = []
        assessment_year = int(self.current_short_date.split('-')[-1])

        if int(self.current_short_date.split("-")[1]) != 12:
            self.projection_dates.append(f'{assessment_year}-12-01')
            self.new_assessment_has_annual = True

        if int(self.assessment_models['MODEL_ASSESSMENT_YEARS'][0]['ANNUAL_ATTRIBUTE']):
            self.projection_dates.extend([f'{assessment_year + year}-01-01' for year in range(1, model_projection_years)])
        else:
            self.projection_dates.extend([f'{assessment_year + year}-01-01' for year in range(1, model_projection_years + 1)])
        logger.info(f"[calculate_assessment_projection_dates] Las fechas proyectadas serían:\n{self.projection_dates}")


    @handler_wrapper('Organizando los datos construídos a dataframes', 'Dataframes creados con exito', 'Error creando dataframes de guardado', 'Error creando tablas de datos base')
    def upload_new_assessment_dates(self):
        dates_records = []
        dates_records.extend([{'DATES': datetime.datetime.strptime(str(date), '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S') , 'PROPERTY': 'PROJECTION'} for date in self.projection_dates])
        dates_records.extend([{'DATES': datetime.datetime.strptime(str(date), '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'), 'PROPERTY': 'HISTORIC'} for date in self.historic_dates]) #para ahorrar procesamiento se puede cambiar este str(date) por un combine entre fecha date y H-M-S en ceros
        self.df_and_upload(dates_records, 'ASSESSMENT_DATES')
        self.historic_dates = [datetime.datetime.strptime(str(date), '%Y-%m-%d') for date in self.historic_dates]
        
        
    @handler_wrapper('Adquiriendo los datos originales de puc para los archives encontrados', 'Datos originales adquiridos y distribuidos con exito', 'Error adquiriendo y organizando los datos originales de puc', 'Error procesando los nuevos pucs')
    def new_assessment_summary_builder(self):
        new_assessment_archives_str = str(self.new_assessment_historic_archives).replace('[', '').replace(']', '')
        query = f"SELECT ID_ARCHIVE, ACCOUNT_NUMBER, CHECKED_BALANCE, ACCOUNT_NAME FROM ORIGINAL_PUC WHERE ID_ARCHIVE IN ({new_assessment_archives_str})"
        logger.info(f'[new_assessment_summary_builder] Query para traer los datos de los pucs originales para los archives encontrados:\n{query}')
        rds_data = self.db_connection.execute(text(query)) #, {'new_assessment_archives_str': new_assessment_archives_str}
        full_historic_asessment_data = [row._asdict() for row in rds_data.fetchall()]
        if not full_historic_asessment_data:
            self.detailed_raise = "Existe el archive de puc pero no se hallaron datos internos como cuentas, saldos y nombres"
            raise Exception (ImportError)
        
        full_historic_asessment_data = self.balance_pucs_accounts(full_historic_asessment_data)

        for master_row in self.assessment_models['MODEL_USER_CLASSIFICATION']:
            #Acá estoy filtrando los numeros de cuentas de todos los archives que empiecen como el master row
            assessment_rows = [item for item in full_historic_asessment_data if item['ACCOUNT_NUMBER'].startswith(master_row['ACCOUNT_NUMBER'])]
            for filtered_row in assessment_rows:
                filtered_row['ID_RAW_CLASSIFICATION'] = master_row['ID_RAW_CLASSIFICATION'] #las cuentas filtradas son clasificadas como el master row
                
        for row in full_historic_asessment_data:
            row['ACCOUNT_NAME'] = f"{row['ACCOUNT_NAME']} ({row['ACCOUNT_NUMBER']})"
            row['CHECKED_BALANCE'] = float(row['CHECKED_BALANCE'])
            row['ID_RAW_CLASSIFICATION'] = row.get('ID_RAW_CLASSIFICATION', self.classification_id_dict['No aplica'])
            self.archive_pucs_data[row['ID_ARCHIVE']] =  self.archive_pucs_data.get(row['ID_ARCHIVE'], []) #si el directorio self.archive_pucs_data no tiene el id_archive, lo crea como una lista vacía, si existe se auto clona los datos; es como si estuviera repartiendo los pucs dependiendo su ID_ARCHIVE
            self.archive_pucs_data[row['ID_ARCHIVE']].append(row)
        

    @handler_wrapper('Balanceando numeros de cuentas entre pucs historicos', 'Balanceado de pucs historics exitoso', 'Error balanceando cuentas de pucs historicos', 'Error construyendo cuentas para balanceado de pucs historicos')
    def balance_pucs_accounts(self, full_historic_asessment_data):
        archives_accounts_directory = {archive: list() for archive in self.new_assessment_historic_archives}
        all_historic_account_numbers = set()
        for row in full_historic_asessment_data:
            all_historic_account_numbers.add(row['ACCOUNT_NUMBER'])
            archives_accounts_directory[row['ID_ARCHIVE']].append(row['ACCOUNT_NUMBER'])
        for archive, this_archive_accounts in archives_accounts_directory.items():
            this_archive_missing_accounts = [item for item in all_historic_account_numbers if item not in this_archive_accounts]
            logger.warning(f'[balance_pucs_accounts] al archive {archive} le hacen falta las cuentas {this_archive_missing_accounts}')
            for account_number in this_archive_missing_accounts:
                full_historic_asessment_data.append({'ACCOUNT_NUMBER': account_number, 'ACCOUNT_NAME': 'Cuenta creada para balanceo de pucs historicos', 'CHECKED_BALANCE': 0, 'ID_RAW_CLASSIFICATION':self.classification_id_dict['No aplica'], 'ID_ARCHIVE': archive})
        return full_historic_asessment_data

        
    @handler_wrapper('Procesando los pucs historicos de recurrencia', 'Pucs historicos de recurrencia procesados con exito', 'Error procesando pucs historicos de recurrencia', 'Error procesando pucs de recurrencia')
    def master_process_recurrence_pucs(self):
        self.second_grade_classifications = ['Amortización del periodo', 'Amortización acumulada', 'Depreciación del periodo', 'Depreciación acumulada', 'Intangibles', 'Propiedad, planta y equipo']
        for archive, data in self.archive_pucs_data.items():
            self.archive_purged_accounts = []
            self.archive_summaries = []
            self.process_puc_checked(archive, data)
            self.process_puc_summary(archive, data)
            self.process_summary_parents(archive)
            
    
    @debugger_wrapper('Error calculando cuentas de puc', 'Error procesando pucs historicos')
    def process_puc_checked(self, archive, data):
        archive_accounts_classifications = {item['ACCOUNT_NUMBER']: item['ID_RAW_CLASSIFICATION'] for item in data}
        erasing_accounts = []
        for account, raw_classification in archive_accounts_classifications.items():
            all_parents = [row for row in archive_accounts_classifications if (account.startswith(row) and account != row)]
            if not all_parents:
                continue
            closest_parent_classification = archive_accounts_classifications[all_parents[-1]]
            if raw_classification == closest_parent_classification:
                if self.easy_classification_dict.get(raw_classification, 'No aplica') in self.second_grade_classifications:
                    if len(all_parents) == 1:
                        erasing_accounts.append(all_parents[-1])
                        continue
                    grand_parents_classification = [archive_accounts_classifications[parent] for parent in all_parents][-2]
                    if  grand_parents_classification == raw_classification:
                        erasing_accounts.append(account)
                        continue
                    else:
                        erasing_accounts.append(all_parents[-1])
                        continue
                erasing_accounts.append(account)
                
        for account in erasing_accounts:
            archive_accounts_classifications.pop(account, None)
            
        to_db_accounts = [item for item in data if item['ACCOUNT_NUMBER'] in archive_accounts_classifications]
        self.archive_purged_accounts = [self.purge_accounts(account_row, to_db_accounts) for account_row in to_db_accounts]
        self.assessment_checked_records.extend(asdict(assessment_checked_object(archive, item['ID_RAW_CLASSIFICATION'], item['ACCOUNT_NUMBER'], item['CHECKED_BALANCE'], item['purged_balance'], item['ACCOUNT_NAME'], item['hint'])) for item in self.archive_purged_accounts if item['ID_RAW_CLASSIFICATION'] != 0)


    @debugger_wrapper('Error purgando cuentas de puc', 'Error calculando disminuciones entre cuentas inferiores')
    def purge_accounts(self, item, accounts_items):
        different_sub_accounts = [row for row in accounts_items if (row['ACCOUNT_NUMBER'].startswith(item['ACCOUNT_NUMBER']) and row['ACCOUNT_NUMBER'] != item['ACCOUNT_NUMBER']) ]
        sub_accounts_levels = sorted(len(row['ACCOUNT_NUMBER']) for row in different_sub_accounts )
        substracted_accounts = []
        sub_accounts_subtraction = 0
        item['hint'] = item['ACCOUNT_NUMBER']
        for level in sub_accounts_levels:
            this_level_accounts = [row for row in different_sub_accounts if (len(row['ACCOUNT_NUMBER']) == level and not row['ACCOUNT_NUMBER'].startswith(tuple(substracted_accounts)))]
            substracted_accounts.extend(row['ACCOUNT_NUMBER'] for row in this_level_accounts)
            sub_accounts_level_values = sum(row['CHECKED_BALANCE'] for row in this_level_accounts)
            sub_accounts_subtraction += sub_accounts_level_values
        if sub_accounts_subtraction:
            logger.info(f'[purge_accounts] La cuenta {item["ACCOUNT_NUMBER"]} tuvo como substracciones a {substracted_accounts}')
            item['hint'] = f"{item['hint']} - ({'+'.join(substracted_accounts)})"
        item['purged_balance'] = item['CHECKED_BALANCE'] - sub_accounts_subtraction
        return item
            
            
    @debugger_wrapper('Error calculando summaries de historicos', 'Error calculando summaries')
    def process_puc_summary(self, archive, data):
        raw_classifications_ids = set(item['ID_RAW_CLASSIFICATION'] for item in self.archive_purged_accounts if item['ID_RAW_CLASSIFICATION'] != 0)
        for raw_id in raw_classifications_ids:
            classification_items = [item for item in self.archive_purged_accounts if item['ID_RAW_CLASSIFICATION'] == raw_id]
            classification_hint = ' + '.join([item['hint'] for item in classification_items])
            classification_value = sum(item['purged_balance'] for item in classification_items)
            self.archive_summaries.append({'id': raw_id, 'value': classification_value})
            self.classification_summary_records.append(asdict(classification_summary_object(archive, raw_id, classification_value, classification_value, classification_hint)))

    @debugger_wrapper('Error agregando cuentas padres a summaries', 'Error calculando cuentas padres')
    def process_summary_parents(self, archive):
        raw_parents_classifications = {item['CLASSIFICATION']:item['ID'] for item in self.raw_classification if item['IS_PARENT']}
        parents_calculated = {parent: 0 for parent in raw_parents_classifications}
        parents_calculated_hints = {parent: '' for parent in raw_parents_classifications}
        for summary in self.archive_summaries:
            summary_classification = self.easy_classification_dict[summary['id']]
            found_parent = next((key for key in raw_parents_classifications if summary_classification.startswith(key) and summary_classification != key), False)
            if not found_parent:
                continue
            parents_calculated[found_parent] = parents_calculated[found_parent] + summary['value']
            parents_calculated_hints[found_parent] = parents_calculated_hints[found_parent] + summary_classification

        for parent_name, id in raw_parents_classifications.items():
            
            self.classification_summary_records.append(asdict(classification_summary_object(archive, id, parents_calculated[parent_name], parents_calculated[parent_name], parents_calculated_hints[parent_name])))


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)