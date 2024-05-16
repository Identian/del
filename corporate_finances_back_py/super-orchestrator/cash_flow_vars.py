
cash_flow_all_items = ['Utilidad neta', 
                        'Impuestos de renta', 
                        'Intereses/gasto financiero',
                        'EBIT', 
                        'Impuestos operacionales', 
                        'UODI (NOPAT)', 
                        'Depreciación del Periodo', 
                        'Deterioro', 
                        'Amortización del Periodo', 
                        'Depreciación Capex',
                        'Otros movimientos que no son salida ni entrada de efectivo operativos', 
                        'Flujo de caja bruto', 
                        'Capital de trabajo', 
                        'CAPEX', 
                        'Otros movimientos netos de activos operativos que afecta el FCLO', 
                        'Otros ingresos y egresos operativos', 
                        'Flujo de caja Libre Operacional', 
                        'Intereses/gasto financiero FC', 
                        'Impuestos no operacionales', 
                        'Variación de deuda',
                        'Deuda de tesorería del periodo',
                        'Deuda de tesorería acumulada',
                        'Otros movimientos netos de activos operativos que afecta el FCLA', 
                        'Otros ingresos y egresos no operativos',
                        'Flujo de caja del accionista', 
                        'Aportes de capital social u otros', 
                        'Dividentos en efectivo', 
                        'Flujo de caja del periodo', 
                        'Saldo de caja inicial', 
                        'Saldo de caja final', 
                        'Check']


pyg_names = ['Utilidad antes de impuestos',
            'Impuestos de renta',
            'Intereses/gasto financiero',
            'Deterioro',
            'Otros ingresos y egresos operativos',
            'Otros ingresos y egresos no operativos',
            'Utilidad neta',
            'EBIT']


capex_items = ['Depreciación del Periodo', 'Amortización del Periodo']


cash_flow_totals = {'Flujo de caja bruto':{'dependencies': ['UODI (NOPAT)','Depreciación del Periodo','Deterioro','Amortización del Periodo', 'Depreciación Capex','Otros movimientos que no son salida ni entrada de efectivo operativos'],
                                            'is_sum': [1,1,1,1,1,-1]},
                    'Flujo de caja Libre Operacional':{'dependencies':['Flujo de caja bruto', 'Capital de trabajo', 'CAPEX', 'Otros movimientos netos de activos operativos que afecta el FCLO', 'Otros ingresos y egresos operativos'],
                                            'is_sum':[1,-1,-1,-1,1]}, #revisar si FCLO suma o resta
                    'Flujo de caja del accionista':{'dependencies':['Flujo de caja Libre Operacional', 'Intereses/gasto financiero FC', 'Impuestos no operacionales', 'Variación de deuda', 'Deuda de tesorería del periodo', 'Otros movimientos netos de activos operativos que afecta el FCLA', 'Otros ingresos y egresos no operativos'], 
                                            'is_sum':[1,-1,-1,1,1,-1,1]},
                    'Flujo de caja del periodo':{'dependencies':['Flujo de caja del accionista','Aportes de capital social u otros', 'Dividentos en efectivo'],
                                            'is_sum':[1,1,-1]}
                    }
                    