
from decorators import handler_wrapper, timing, debugger_wrapper
import logging
from datetime import datetime, timedelta
import sys
import math
import copy
import pandas as pd
from sqlalchemy import text

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class builder_class():

    @handler_wrapper('Ejecutando builder', 'Builder ejecutado con exito', 'Error ejecutando builder de puc historico', 'Error ejecutando builder de puc historico')
    def historic_pucs_builder(self):
        self.parent_accounts_items = {'Ingresos operacionales': ['Ingresos operacionales 1', 'Ingresos operacionales 2', 'Ingresos operacionales 3', 'Ingresos operacionales 4', 'Ingresos operacionales 5'], 'Gastos operacionales': ['Gastos operacionales 1', 'Gastos operacionales 2', 'Gastos operacionales 3', 'Otros ingresos/egresos operativos']}
        self.special_classifications = ["Depreciación del periodo", "Depreciación acumulada", "Propiedad, planta y equipo", "Intangibles", "Amortización acumulada", "Amortización del periodo"]

        self.purged_items = list()
        self.calculated_summaries = list()

        assessment_archives = set(row['ID_ARCHIVE'] for row in self.full_historic_data)
        for archive in assessment_archives:
            self.id_archive_processing = archive
            self.full_records = [row for row in self.full_historic_data if row['ID_ARCHIVE'] == archive]

            self.classification_shooter()
            self.parent_accounts_shooter()
            self.special_classifications_shooter()
        
        self.merge_classifications_ids()
        self.fix_data_to_bd()
        self.upload_results_to_bd()


    @handler_wrapper('Inicializando disparador de clasificaciones', 'Disparador de clasificaciones terminado con exito', 'Error en el disparador de clasificaciones', 'Error calculando clasificacion')
    def classification_shooter(self):
        for master_classification in self.classification_id_dict:
            self.basic_seeker(master_classification)


    @debugger_wrapper('Error filtrando las clasificaciones requeridas', 'Error filtrando clasificaciones')
    def basic_seeker(self, classification_to_find):
        logger.info(f'[basic_seeker] Calculando la clasificacion: "{classification_to_find}"')
        group_filter = lambda item: item.get('classification', 'No aplica') == classification_to_find
        found_items = list(filter(group_filter, self.full_records))
        if not found_items or classification_to_find == 'No aplica':
            logger.info(f'[basic_seeker] No se encontraron cuentas con clasificacion {classification_to_find}')
            return
        found_items = self.filter_items_to_min_level(found_items) 
        logger.warning(f"[basic_seeker] Los items con la clasificacion {classification_to_find} son: \n{found_items}")
        found_items = copy.deepcopy(found_items)
        self.individual_records_modifier(found_items)
        self.classification_summary_calculator(classification_to_find)
        

    @handler_wrapper('Inicializando disparador de clasificaciones padre', 'Disparador de cuentas padre terminado con exito', 'Error en el disparador cuentas padre', 'Error calculando cuentas padre de subs')
    def parent_accounts_shooter(self):
        [self.calculate_parents_accounts(parent_account, sub_classifications) for parent_account, sub_classifications in self.parent_accounts_items.items()]
    

    def calculate_parents_accounts(self, parent_account, sub_classifications):
        purged_items = list(filter(lambda item: item['classification'] in sub_classifications, self.purged_items))
        self.calculated_summaries.append({'classification': parent_account, **self.calculate_summary_groups(purged_items), 'ID_ARCHIVE': self.id_archive_processing})
    

    @handler_wrapper('Recalculando items de clasificaciones especiales', 'Clasificaciones especiales recalculadas con exito', 'Error al calcular cuentas especiales', 'Error calculando cuentas especiales')
    def special_classifications_shooter(self):
        logger.warning(f'purged_items antes de especiales: \n{self.purged_items}')
        [self.special_seeker(master_classification) for master_classification in self.special_classifications]
        logger.warning(f'purged_items despues de especiales: \n{self.purged_items}')


    @debugger_wrapper('Fallo calculando clasificaciones especiales', 'Error calculando clasificaciones de tipo especial')
    def special_seeker(self, classification_to_find):
        logger.info(f'[special_seeker] Calculando la clasificacion: "{classification_to_find}"')
        f = lambda item : item.get('classification', 'No aplica') == classification_to_find
        found_items = list(filter(f, self.full_records))
        if found_items:
            sorted_unique_found_levels = sorted(set(item['nivel'] for item in found_items))
            if len(sorted_unique_found_levels) > 1:
                level_to_search = sorted_unique_found_levels[1]
                self.special_purged_accounts_eraser(classification_to_find)
                g = lambda item : item['nivel'] == level_to_search
                found_items = list(filter(g, found_items))
                found_items = copy.deepcopy(found_items)
                self.individual_records_modifier(found_items)
                return

            logger.info(f'[special_seeker] Todos los items de la clasificacion {classification_to_find} tienen solo un nivel')
            return 

        logger.info(f'[special_seeker] No se encontraron cuentas con clasificacion {classification_to_find}')


    def special_purged_accounts_eraser(self, classification_to_erase):
        self.purged_items = list(filter(lambda item:item['classification'] != classification_to_erase or item['ID_ARCHIVE'] != self.id_archive_processing, self.purged_items))


    def individual_records_modifier(self, found_items):
        self.purged_items +=  list(map(self.purge_individual_item, found_items))
    

    def classification_summary_calculator(self, classification_to_find):
        purged_items = list(filter(lambda item: item['classification'] == classification_to_find and item['ID_ARCHIVE'] == self.id_archive_processing, self.purged_items))
        self.calculated_summaries.append({'classification': classification_to_find, **self.calculate_summary_groups(purged_items), 'ID_ARCHIVE': self.id_archive_processing})


    @debugger_wrapper('Error purgando cuentas individuales', 'Error purgando cuentas de puc')
    def purge_individual_item(self, master_account):
        master_account['ACCOUNT_NAME'] = f"{master_account['ACCOUNT_NAME'].strip()} ({master_account['ACCOUNT_NUMBER']})" 
        master_account['hint'] = f"{master_account['ACCOUNT_NUMBER']} "
        master_account['ID_ARCHIVE'] = self.id_archive_processing
        filter_machine = self.create_sub_accounts_filter(master_account)
        self.sub_classifications_pool = list(filter(lambda item: all(f(item) for f in filter_machine), self.full_records))
        self.sub_classifications_pool.sort(key = lambda item: item['nivel'], reverse = False) #Con esto me aseguro que el set de subclasificaciones esté en orden de nivel y que no purgaré un nivel 10 antes de purgar un nivel 6
        sub_classifications = self.classifications_set_ordered(self.sub_classifications_pool)
        logger.warning(f"[purge_individual_item] sub clasificaciones encontradas para la cuenta {master_account['ACCOUNT_NUMBER']}: \n{sub_classifications}")
        
        for classification in sub_classifications:
            master_account['hint'] = master_account.get('hint', master_account['ACCOUNT_NUMBER']) #ver si esto lo puedo borrar
            logger.warning(f"[purge_individual_item] Cuenta {master_account['ACCOUNT_NUMBER']} antes de modificacion por clasificacion {classification}: \nValor: {master_account['value']}\nHint: {master_account['hint']}")
            sub_classification_items = [item for item in self.sub_classifications_pool if item['classification'] == classification]
            sub_classification_items = self.filter_items_to_min_level(sub_classification_items)

            for sub_item in sub_classification_items:
                master_account = self.apply_sub_item_to_master(master_account, sub_item)

            logger.warning(f"[purge_individual_item] Cuenta {master_account['ACCOUNT_NUMBER']} post modificacion de clasificacion {classification}: \nvalor: {master_account['value']}, hint: {master_account['hint']}")
        return master_account


    def create_sub_accounts_filter(self, master_account):
        filters = []
        filters.append(lambda item: item['ACCOUNT_NUMBER'].startswith(master_account['ACCOUNT_NUMBER']))
        filters.append(lambda item: item['classification'] != master_account['classification'])
        filters.append(lambda item: item['nivel'] > master_account['nivel'])
        #filters.append(lambda item: item['classification'] != 'No aplica')
        return filters


    @debugger_wrapper('Error filtrando cuentas a minimo nivel', 'Error procesando data')
    def filter_items_to_min_level(self, items):
        nivels = sorted(set([item['nivel'] for item in items]))
        usable_accounts = [ item['ACCOUNT_NUMBER'] for item in items if item['nivel'] == nivels[0] ]
        logger.info(f'[filter_items_to_min_level] usable_accounts original: \n{usable_accounts}')
        
        for nivel in nivels[1:]:
            this_level_items = [item for item in items if item['nivel'] == nivel]
            new_items = [item['ACCOUNT_NUMBER'] for item in this_level_items if not item['ACCOUNT_NUMBER'].startswith(tuple(usable_accounts))] 
            #Acá se están obteniendo las subcuentas que estén clasificadas igual que el subaccount del master pero que no sean subcuentas entre sí
            #suponga: master account 11 efectivo, subcuenta 1105 ppe, subcuenta 131015 ppe; estas dos subcuentas no son subcuentas entre sí, por eso ambas 
            #deben restarse del master account
            #si la subcuentas fueran 1110 ppe y 111020 ppe, debería tenerse en cuenta unicamente la 1110
            logger.info(f'[filter_items_to_min_level] se agregan las siguientes cuentas: \n{new_items}')
            usable_accounts.extend(new_items)
        return list(filter(lambda item: item['ACCOUNT_NUMBER'] in usable_accounts , items))



    def classifications_set_ordered(self, sub_classifications_pool):
        return list(dict.fromkeys(item['classification'] for item in sub_classifications_pool)) #Esto es para hacer un SET 
        #de las clasificaciones pero sin perder el orden de las mismas, porque ya están organizadas por nivel, requiero 
        #sacar las clasificaciones sin repetirse pero sin desorganizarse, que es lo que haría un set()


    def apply_sub_item_to_master(self, master_account, sub_item):
        master_account['value'] = master_account['value'] - sub_item['value']
        master_account['hint'] = master_account['hint'] + f"-{sub_item['ACCOUNT_NUMBER']} " 
        
        self.sub_classifications_pool = list(filter(lambda item: not item['ACCOUNT_NUMBER'].startswith(sub_item['ACCOUNT_NUMBER']), self.sub_classifications_pool))
        #recordar que hay un sub_classification_items que son las sub cuentas con una clasificaciones, pero hay un sub_classifications_pool que contiene TODAS las subcuentas con 
        #multiples clasificaciones, las subcuentas con la misma clasificacion que se deban omitir ya se están omitiendo en filter_items_to_min_level, pero si hay sub clasificaciones 
        #con clasificaciones differentes en el pool, estas tambien se deben omitir, por eso es tan importante que la depuración se realice en niveles inferiores y luego a superiores
        return master_account
        
        
    #OJO esta definicion no se está usando
    def calculate_sub_grouping(self, item):
        value = 0
        accounts_used_log = ''

        #logger.warning(f"{item}")
        if (item['nature'] == 'Debito' and item['ACCOUNT_NUMBER'][0] in ['1', '2', '3']) or (item['nature'] == 'Credito' and item['ACCOUNT_NUMBER'][0] in ['4', '5', '6']):
            value = value + item['value']
            accounts_used_log = accounts_used_log + f"+{item.get('hint', item.get('ACCOUNT_NUMBER'))} "
        if (item['nature'] == 'Credito' and item['ACCOUNT_NUMBER'][0] in ['1', '2', '3']) or (item['nature'] == 'Debito' and item['ACCOUNT_NUMBER'][0] in ['4', '5', '6']):
            value = value - item['value']
            accounts_used_log = accounts_used_log + f"-{item.get('hint', item.get('ACCOUNT_NUMBER'))} "
        return {'value': value, 'hint':accounts_used_log}
    
    
    def calculate_summary_groups(self, found_purged_items):
        #ya que no se está teniendo en cuenta naturaleza y capitulos para el df2, 
        #lo ideal es que al sumar estas cuentas purgadas, sí se tenga en cuenta estas cosas
        #usar el algoritmo de calculate_sub_grouping
        summary_value = sum(item['value'] for item in found_purged_items)
        summary_hint = ' + '.join(item['hint'] for item in found_purged_items)
        return {'value': summary_value, 'hint':summary_hint}


    @handler_wrapper('Emergiendo IDs de los nombres de clasificacion','Ids emergidos correctamente', 'Error emergiendo IDs de clasificacion', 'Error agrupando datos')
    def merge_classifications_ids(self):
        logger.warning(f'Este es mi elemento self.inverted_raw_classifications {self.classification_id_dict}')
        for item in self.calculated_summaries:
            item['ID_RAW_CLASSIFICATION'] = self.classification_id_dict[item['classification']]
        for item in self.purged_items:
            item['ID_RAW_CLASSIFICATION'] = self.classification_id_dict[item['classification']]


    @handler_wrapper('Arreglando dataframes para subida a bd', 'Arreglo de dataframes terminado', 'Error arreglando dataframes', 'Error operando datos')
    def fix_data_to_bd(self):
        self.summaries_df = pd.DataFrame.from_records(self.calculated_summaries)
        self.purged_items_df = pd.DataFrame.from_records(self.purged_items)

        logger.warning(f"[fix_dataframes] summaries antes de fix: \n{self.summaries_df.to_string()}")
        self.summaries_df['ID_ASSESSMENT'] = self.id_assessment
        self.summaries_df['ANNUALIZED'] = self.summaries_df['value']
        self.summaries_df.rename(columns={'value': 'CALCULATED_BALANCE', 'hint': 'HINT'},inplace=True)
        self.summaries_df.drop(['classification'], axis=1, inplace=True)
        #De este me falta el merge con id_raw_classifications
        
        logger.warning(f"[fix_dataframes] purgeds antes de fix: \n{self.purged_items_df.to_string()}")
        self.purged_items_df['ID_ASSESSMENT'] = self.id_assessment
        self.purged_items_df['ANNUALIZED'] = self.purged_items_df['value']
        self.purged_items_df.rename(columns={'value': 'CALCULATED_BALANCE', 'hint': 'HINT', 'nature': 'NATURE', 'name': 'ACCOUNT_NAME'},inplace=True)
        self.purged_items_df.drop(['nivel', 'classification'], axis=1, inplace=True)  #este chapter es importante para despues, sería mejor no borrarlo
        #De este me falta el merge con id_raw_classifications

        logger.warning(f"[fix_dataframes] summaries despues de fix: \n{self.summaries_df.to_string()}")
        logger.warning(f"[fix_dataframes] purgeds despues de fix: \n{self.purged_items_df.to_string()}")
        #TODO: Tengo que sí o sí arreglar los hints y revisar los casos
        

    @handler_wrapper('Cargando datos a bd', 'Datos cargados a bd satisfactoriamente', 'Error cargando datos a bd', 'Error cargando resultados a base de datos')
    def upload_results_to_bd(self):
        self.summaries_df.to_sql(name='CLASSIFICATION_SUMMARY', con=self.db_connection, if_exists='append', index=False)
        self.purged_items_df.to_sql(name='ASSESSMENT_CHECKED', con=self.db_connection, if_exists='append', index=False)