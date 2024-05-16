import logging
import sys
import datetime

from dataclasses_box import capex_values_object, projected_debt_object, fixed_assets_projected_item, modal_windows_projected_object, pyg_item_object, pyg_projected_object, asdict

from _calc_capex_summary import capex_summary_class
from _calc_cash_flow import cash_flow_class
from _calc_debt import debt_class
from _calc_fixed_assets import fixed_assets_class
from _calc_new_capex import new_capex_class
from _calc_other_projections import other_projections_class
from _calc_patrimony import patrimony_class
from _calc_pyg import pyg_class
from _calc_pyg_projections import pyg_projections_class
from _calc_working_capital import working_capital_class
from _calc_assessment import assessment_class

from decorators import handler_wrapper
from sqlalchemy import text
from models_tables import ordered_atributes_tables

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class recalculating_methods_class(fixed_assets_class,patrimony_class,working_capital_class,other_projections_class,debt_class,new_capex_class,pyg_class,pyg_projections_class,capex_summary_class,cash_flow_class, assessment_class):
    def recalculating_starter(self):
        self.get_purged_items()
        self.get_summary_results()
        self.get_assessment_steps()
        self.annualize_checking()
        self.delete_from_bd("NOTES", '')  #TODO: ver qué pasa con esto y con lo de modelo transaccional

        for context, fun in self.context_methods.items():
            try:
                self.current_context = context
                fun()
            except Exception as e:
                self.orchestrator_state_update(f"Error: {context}")
                logger.error(f"[recalculating_starter] Falló la ejecución del contexto {context}")
                self.noting_list.append({"TASK": self.current_context,"NOTE": f"Falló la ejecución de {context}, motivo: {str(e)}"})
                continue

    @handler_wrapper("Obteniendo datos de puc purgados", "Datos de puc obtenidos", "Error obteniendo datos de puc", "Error al buscar datos de puc purgados")
    def get_purged_items(self):
        query = f"""SELECT A.ACCOUNT_NUMBER AS account, B.CLASSIFICATION AS classification, A.ANNUALIZED AS value, A.ACCOUNT_NAME AS NAME, A.HINT 
        FROM ASSESSMENT_CHECKED A, RAW_CLASSIFICATION B, ARCHIVE C 
        WHERE A.ID_RAW_CLASSIFICATION = B.ID AND A.ID_ARCHIVE = C.ID AND ID_ASSESSMENT = {self.id_assessment} ORDER BY C.INITIAL_DATE"""

        logger.info(f"[get_purged_items] Query a base de datos para obtener los datos de puc calculados:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        self.purged_items = [item._asdict() for item in rds_data.fetchall()]
        logger.info(f"[get_purged_items] Datos de cuentas traídas desde bd:\n{self.purged_items}")
        for item in self.purged_items:
            item["value"] = float(item["value"])

        logger.warning(f"[get_purged_items] datos de cuentas post procesamiento inicial:\n{self.purged_items}")

    @handler_wrapper("Obteniendo datos summary de clasificacion","summaries de clasificacion obtenidos","Error obteniendo summaries","Error al buscar informacion de sumaries")
    def get_summary_results(self):
        # trunk-ignore(bandit/B608)
        query = f"""SELECT C.ID AS id_archive, B.CLASSIFICATION, A.HINT AS hint, A.ANNUALIZED AS value, B.IS_PARENT 
FROM CLASSIFICATION_SUMMARY A, RAW_CLASSIFICATION B, ARCHIVE C
WHERE A.ID_RAW_CLASSIFICATION = B.ID AND A.ID_ARCHIVE = C.ID
AND ID_ASSESSMENT = {self.id_assessment} ORDER BY C.INITIAL_DATE"""

        logger.info(f"[get_archive_info] Query a base de datos para obtener los summaries de clasificacion calculados:\n {query}")
        rds_data = self.db_connection.execute(text(query))

        self.summary_data = [dict(item) for item in rds_data.mappings().all()]
        for item in self.summary_data:
            item["value"] = float(item["value"])
        self.all_assessment_classifications = tuple(item["CLASSIFICATION"] for item in self.summary_data)
        logger.warning(f"[get_summary_results] Summaries encontrados para el proceso de valoración:\n{self.summary_data}")

    @handler_wrapper('Obteniendo los steps del proceso de valoración', 'Steps del proceso de valoración obtenidos', 'Error obteniendo steps del proceso de valoración', 'Error obteniendo steps del proceso de valoración')
    def get_assessment_steps(self):
        query = f"""SELECT SERVICE FROM ASSESSMENT_STEPS WHERE ID_ASSESSMENT = {self.id_assessment}"""
        logger.info(f"[get_assessment_steps] Query a base de datos para obtener los steps existentes del proceso de valoración:\n {query}")
        rds_data = self.db_connection.execute(text(query))
        self.steps_list = [row.SERVICE for row in rds_data.fetchall()]



    @handler_wrapper("Revisando si se requiere modificaciones de atributos por anualización","Chequeo de anualización terminado", "Error realizando chequeo de anualización","Error realizando anualización")
    def annualize_checking(self):
        if self.context == "full_recurrency":
            if self.assessment_models["MODEL_ASSESSMENT_YEARS"][0]["ANNUAL_ATTRIBUTE"]== 1 and not self.new_assessment_has_annual:
                #TODO: aca toca poner una función para eliminar un año de proyeccion
                logger.warning("[annualize_checking] Se encontró que el modelo es anualizado y el nuevo proceso de valoración no requiere anualización")
                logger.debug(f'[annualize_checking] modelos previos a borrado de atributo de anualización:\n{self.assessment_models}')
                self.delete_all_annual_attributes()
                logger.debug(f'[annualize_checking] modelos sin atributos de anualización:\n{self.assessment_models}')
            elif self.assessment_models["MODEL_ASSESSMENT_YEARS"][0]["ANNUAL_ATTRIBUTE"]== 0 and self.new_assessment_has_annual:
                #TODO: aca toca poner una función que agregué una proyeccion, el de anualización
                self.add_annual_attributes()


    @handler_wrapper("Eliminando atributo de anualización de todos los modelos","Atributo de anualización eliminado con éxito","Error eliminando atributo de anualización","Error procesando modelo anualizacido y proceso de valoración que no requiere anualizacion")
    def delete_all_annual_attributes(self):
        #MODEL_FIXED_ASSETS
        for row in self.assessment_models['MODEL_FIXED_ASSETS']:
            row['PROJECTED_YEARS'] = row['PROJECTED_YEARS'] - 1

        #MODEL_CAPEX_VALUES
        self.assessment_models['MODEL_CAPEX_VALUES'] = self.assessment_models['MODEL_CAPEX_VALUES'][1:]

        #MODEL_FCLO_DISCOUNT #no debo borrar esta propiedad porque la cantidad de atributos va estar ok
        #self.assessment_models['MODEL_FCLO_DISCOUNT'] = self.assessment_models['MODEL_FCLO_DISCOUNT'][1:]

        #MODEL_PROJECTED_DEBT
        cleaned_items = tuple()
        for row in self.assessment_models['MODEL_PROJECTED_DEBT']:
            if (row['ACCOUNT_NUMBER'], row['ALIAS_NAME']) not in cleaned_items:
                cleaned_items = cleaned_items + ((row['ACCOUNT_NUMBER'], row['ALIAS_NAME']),)
                row['DELETE_ROW'] = True
                #logger.debug(f"[delete_all_annual_attributes] estoy pidiendo quemar el row: {row} porque {(row['ACCOUNT_NUMBER'], row['ALIAS_NAME'])} no está en {cleaned_items}")
            else:
                row['DELETE_ROW'] = False
        self.assessment_models['MODEL_PROJECTED_DEBT'] = [row for row in self.assessment_models['MODEL_PROJECTED_DEBT'] if not row['DELETE_ROW']]

        #MODEL_PROJECTED_FIXED_ASSETS
        cleaned_items = tuple()
        for row in self.assessment_models['MODEL_PROJECTED_FIXED_ASSETS']:
            if (row['ID_ITEMS_GROUP']) not in cleaned_items:
                cleaned_items = cleaned_items + (row['ID_ITEMS_GROUP'], )
                row['DELETE_ROW'] = True
            else:
                row['DELETE_ROW'] = False
        self.assessment_models['MODEL_PROJECTED_FIXED_ASSETS'] = [row for row in self.assessment_models['MODEL_PROJECTED_FIXED_ASSETS'] if not row['DELETE_ROW']]

        #MODEL_MODAL_WINDOWS_PROJECTED
        cleaned_items = tuple()
        for row in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED']:
            if (row['CONTEXT_WINDOW'], row['ACCOUNT_NUMBER']) not in cleaned_items:
                cleaned_items = cleaned_items + ((row['CONTEXT_WINDOW'], row['ACCOUNT_NUMBER']),)
                row['DELETE_ROW'] = True
            else:
                row['DELETE_ROW'] = False
        self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] = [row for row in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] if not row['DELETE_ROW']]

        #MODEL_PROJECTED_PYG
        cleaned_items = tuple()
        for row in self.assessment_models['MODEL_PROJECTED_PYG']:
            if (row['ID_RAW_PYG']) not in cleaned_items:
                cleaned_items = cleaned_items + (row['ID_RAW_PYG'], )
                row['DELETE_ROW'] = True
            else:
                row['DELETE_ROW'] = False
        self.assessment_models['MODEL_PROJECTED_PYG'] = [row for row in self.assessment_models['MODEL_PROJECTED_PYG'] if not row['DELETE_ROW']]


    @handler_wrapper("Agregando atributo de anualización a todos los modelos", "Atributo de anualización agregado con éxito", "Error agregando atributo de anualización", "Error procesando modelo no anualizado y proceso de valoración que requiere anualizacion")
    def add_annual_attributes(self):
        annualized_date_short = self.projection_dates[0]
        annualized_date_long = datetime.datetime.strptime(annualized_date_short, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')
        #MODEL_FIXED_ASSETS
        for row in self.assessment_models['MODEL_FIXED_ASSETS']:
            row['PROJECTED_YEARS'] = row['PROJECTED_YEARS'] + 1

        #MODEL_CAPEX_VALUES
        self.assessment_models['MODEL_CAPEX_VALUES'] =[asdict(capex_values_object(annualized_date_long, '0', 0, 0))] + self.assessment_models['MODEL_CAPEX_VALUES'][1:]

        #MODEL_FCLO_DISCOUNT
        self.assessment_models['MODEL_FCLO_DISCOUNT'] = [{'ITEM_DATE':annualized_date_long, 'DISCOUNT_PERIOD':0, 'DISCOUNT_RATE':0, 'DISCOUNT_FACTOR':0}] + self.assessment_models['MODEL_FCLO_DISCOUNT']

        #MODEL_PROJECTED_DEBT
        debts_ids = set([(row['ACCOUNT_NUMBER'], row['ALIAS_NAME']) for row in self.assessment_models['MODEL_PROJECTED_DEBT']])
        adding_rows = []
        for pair in debts_ids:
            logger.debug(f'[mira aca] Esto no debería tener diccionarios repetidos:\n{debts_ids}')
            adding_rows.append(asdict(projected_debt_object(pair[0], pair[1], annualized_date_long, 0, 0, 0, 0, 0, 0, '0', '0')))

        self.assessment_models['MODEL_PROJECTED_DEBT'] = adding_rows + self.assessment_models['MODEL_PROJECTED_DEBT']

        #MODEL_PROJECTED_FIXED_ASSETS
        groups_ids = set([row['ID_ITEMS_GROUP'] for row in self.assessment_models['MODEL_PROJECTED_FIXED_ASSETS']])
        adding_rows = []
        for group_id in groups_ids:
            adding_rows.append(asdict(fixed_assets_projected_item(group_id, annualized_date_long, 0, 0, 0, 0)))

        self.assessment_models['MODEL_PROJECTED_FIXED_ASSETS'] = adding_rows + self.assessment_models['MODEL_PROJECTED_FIXED_ASSETS']

        #MODEL_MODAL_WINDOWS_PROJECTED
        modal_pairs = set([(row['ACCOUNT_NUMBER'], row['CONTEXT_WINDOW']) for row in self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED']])
        adding_rows = []
        for pair in modal_pairs:
            logger.debug(f'[mira aca] Esto no debería tener diccionarios repetidos:\n{modal_pairs}')
            adding_rows.append(asdict(modal_windows_projected_object(0, pair[0], annualized_date_long, '0', pair[1])))

        self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED'] = adding_rows + self.assessment_models['MODEL_MODAL_WINDOWS_PROJECTED']

        #MODEL_PROJECTED_PYG
        raw_pyg_ids = set([row['ID_RAW_PYG'] for row in self.assessment_models['MODEL_PROJECTED_PYG']])
        adding_rows = []
        for raw_id in raw_pyg_ids:
            adding_rows.append(asdict(pyg_projected_object(raw_id, annualized_date_long, 0, '0')))
        self.assessment_models['MODEL_PROJECTED_PYG'] = adding_rows + self.assessment_models['MODEL_PROJECTED_PYG']


def get_current_error_line():
    return str(sys.exc_info()[-1].tb_lineno)
