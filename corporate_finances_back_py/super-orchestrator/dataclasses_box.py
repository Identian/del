
from dataclasses import dataclass, asdict#, field #field es para cuando se debe crear algo en __post_init__


@dataclass
class assessment_checked_object:
    ID_ARCHIVE              : int
    ID_RAW_CLASSIFICATION   : int
    ACCOUNT_NUMBER          : str
    CALCULATED_BALANCE      : float
    ANNUALIZED              : float
    ACCOUNT_NAME            : str
    HINT                    : str 
    NATURE                  : str = ''

@dataclass
class classification_summary_object:
    ID_ARCHIVE              : int
    ID_RAW_CLASSIFICATION   : int
    CALCULATED_BALANCE      : float
    ANNUALIZED              : float
    HINT                    : str
    
@dataclass
class capex_values_object:
    CAPEX_NAME              : str
    CALCULATED_DATE         : str
    MANUAL_PERCENTAGE       : str
    CAPEX_SUMMARY           : float
    CAPEX_ACUMULATED        : float

@dataclass
class capex_object:
    USED_ACCOUNT_NAME   : str
    METHOD              : str
    TIME_PERIOD         : str
    PERIODS             : int
    CALCULATION_COMMENT : str

@dataclass
class fclo_object:
    ITEM_DATE           : str
    OPERATIVE_CASH_FLOW : float
    DISCOUNT_PERIOD     : str
    DISCOUNT_RATE       : str
    DISCOUNT_FACTOR     : str
    FCLO                : float

@dataclass
class projected_debt_object:
    ACCOUNT_NUMBER          : str
    ALIAS_NAME              : str
    ITEM_DATE               : int
    INITIAL_BALANCE         : float
    DISBURSEMENT            : float
    AMORTIZATION            : float
    ENDING_BALANCE          : float
    INTEREST_VALUE          : float
    ENDING_BALANCE_VARIATION: float
    RATE_ATRIBUTE           : float
    SPREAD_ATRIBUTE         : float
    
    def coupled_exit (self):
        #TODO: este proy_year puede que necesite solo el año, le estoy mandando el item_date completo
        return {'proy_year': self.ITEM_DATE,'interest': self.INTEREST_VALUE, 'variation': self.ENDING_BALANCE_VARIATION} #acá debo traer el que hace falta en flujo de caja
        

@dataclass
class debt_object:
    ORIGINAL_VALUE          : float
    ACCOUNT_NUMBER          : str
    ALIAS_NAME              : str
    PROJECTION_TYPE         : str
    START_YEAR              : str
    ENDING_YEAR             : str
    DEBT_COMMENT            : float
    RATE_COMMENT            : float
    SPREAD_COMMENT          : float

@dataclass
class coupled_debt_object:
    SUMMARY_DATE    : str
    DISBURSEMENT    : float
    INTEREST_VALUE  : float
    
@dataclass
class fixed_assets_object:
    ID_ITEMS_GROUP              : int
    PROJECTION_TYPE             : int
    ASSET_ACCOUNT               : str
    ACUMULATED_ACCOUNT          : str
    PERIOD_ACCOUNT              : str
    ASSET_ORIGINAL_VALUE        : float
    ACUMULATED_ORIGINAL_VALUE   : float
    PERIOD_ORIGINAL_VALUE       : float
    PROJECTED_YEARS             : int
    CALCULATION_COMMENT         : str
    
    def capex_summary_output(self):
        return {self.ID_ITEMS_GROUP: {'asset_account': self.ASSET_ACCOUNT, 'acumulated_account':self.ACUMULATED_ACCOUNT, 'period_account': self.PERIOD_ACCOUNT}}


@dataclass
class fixed_assets_projected_item:
    ID_ITEMS_GROUP      : int
    PROJECTED_DATE      : str
    ASSET_VALUE         : float
    ACUMULATED_VALUE    : float
    EXISTING_ASSET_VALUE: float
    PERIOD_VALUE        : float

    def capex_output(self):
        return {self.PROJECTED_DATE.split('-')[0]: 
            {'asset': self.ASSET_VALUE, 'acumulated': self.ACUMULATED_VALUE, 'existing': self.EXISTING_ASSET_VALUE, 'period': self.PERIOD_VALUE}}

    def capex_summary_output(self):
        return {'date': self.PROJECTED_DATE, 'acumulated': self.ACUMULATED_VALUE, 'period': self.PERIOD_VALUE, 'id_items_group': self.ID_ITEMS_GROUP, 'asset_value': self.ASSET_VALUE}


@dataclass
class wk_results_object:
    SUMMARY_DATE    : str
    WK_VARIATION    : float
    INTEREST_VALUE  : float = 0
    
@dataclass
class patrimony_results_object:
    SUMMARY_DATE            : str
    SOCIAL_CONTRIBUTIONS    : float
    CASH_DIVIDENDS          : float

@dataclass
class other_modal_results_object:
    SUMMARY_DATE                : str
    OTHER_OPERATIVE_MOVEMENTS   : float
    FCLO                        : float
    FCLA                        : float

@dataclass
class modal_windows_projected_object:
    VALUE           : float
    ACCOUNT_NUMBER  : str
    PROJECTED_DATE  : str
    ATRIBUTE        : str
    CONTEXT_WINDOW  : str

@dataclass
class pyg_item_object:
    ID_RAW_PYG      : int
    ID_DEPENDENCE   : int
    ORIGINAL_VALUE  : float
    PROJECTION_TYPE : str
    COMMENT         : str
        
@dataclass
class pyg_projected_object:
    ID_RAW_PYG      : int
    PROJECTED_DATE  : str
    VALUE           : float
    ATRIBUTE        : str

@dataclass
class capex_summary_object:
    SUMMARY_DATE: str
    OPERATIONAL_INCOME: str
    EXISTING_ASSETS: float
    CAPEX: float
    NEW_CAPEX: float
    PERIOD_DEPRECIATION: float
    PERIOD_AMORTIZATION: float
    ACUMULATED_DEPRECIATION: float
    ACUMULATED_AMORTIZATION: float
    ACUMULATIVE_CHECK: int = 1

@dataclass
class calculated_assessment_object:
    ASSESSMENT_DATE         : str
    INITIAL_DATE            : str
    CURRENT_CLOSING_DATE    : str
    FLOW_HALF_PERIOD        : str
    NEXT_FLOW_HALF_YEAR     : str
    DATES_ADJUST_ATRIBUTE   : str
    DATES_ADJUST_COMMENT    : str
    CHOSEN_FLOW_NAME        : str
    CHOSEN_FLOW_COMMENT     : str
    DISCOUNT_RATE_COMMENT   : str
    VP_FLOWS                : float
    GRADIENT                : str
    NORMALIZED_CASH_FLOW    : float
    DISCOUNT_RATE_ATRIBUTE  : str
    TERMINAL_VALUE          : float
    DISCOUNT_FACTOR         : str
    VP_TERMINAL_VALUE       : float
    ENTERPRISE_VALUE        : float
    FINANCIAL_ADJUST        : float
    TOTAL_NOT_OPERATIONAL_ASSETS    : float
    TOTAL_OPERATIONAL_PASIVES       : float
    ASSETS_COMMENT          : str
    PASIVES_COMMENT         : str
    PATRIMONY_VALUE         : float
    OUTSTANDING_SHARES      : int
    ASSESSMENT_VALUE        : float
    ADJUST_METHOD           : str