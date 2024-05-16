
pyg_all_items = ['Costos (Sin depreciación)', 
                'Depreciación del periodo', 
                'Amortización del periodo', 
                'Otros ingresos operativos', 
                'Otros egresos operativos', 
                'Otros ingresos no operativos', 
                'Otros egresos no operativos', 
                'Depreciación Capex',
                'Deterioro', 
                'Impuestos de renta', 
                'Intereses/gasto financiero', 
                'Ingresos operacionales', 
                'Gastos operacionales', 
                'Otros ingresos y egresos no operativos', 
                'Otros ingresos y egresos operativos', 
                'Utilidad bruta', 
                'EBITDA', 
                'EBIT', 
                'Utilidad antes de impuestos', 
                'Utilidad neta']


pyg_simple_calculations = ['Costos (Sin depreciación)',
                            'Depreciación del periodo',
                            'Depreciación Capex',
                            'Amortización del periodo',
                            'Otros ingresos operativos',
                            'Otros egresos operativos',
                            'Otros ingresos no operativos',
                            'Otros egresos no operativos',
                            'Deterioro',
                            'Impuestos de renta',
                            'Intereses/gasto financiero',
                            'Ingresos operacionales',
                            'Gastos operacionales']
                            

#TODO: Debo arreglar estos totales a los totales de pyg
pyg_partials = {'Otros ingresos y egresos no operativos':{'dependencies': ['Otros ingresos no operativos','Otros egresos no operativos'],
                                            'is_sum': [1,-1]},
                    'Otros ingresos y egresos operativos':{'dependencies':['Otros ingresos operativos','Otros egresos operativos'],
                                            'is_sum':[1,-1]}}
                                            
pyg_totals = {'Utilidad bruta':{'dependencies':['Ingresos operacionales','Costos (Sin depreciación)'], 
                                            'is_sum':[1,-1]},
                    'EBITDA':{'dependencies':['Utilidad bruta','Gastos operacionales','Otros ingresos y egresos operativos'],
                                            'is_sum':[1,-1,1]},
                    'EBIT':{'dependencies':['EBITDA','Depreciación del periodo','Amortización del periodo','Depreciación Capex','Deterioro'],
                                            'is_sum':[1,-1,-1,-1,-1]},
                    'Utilidad antes de impuestos':{'dependencies':['EBIT','Otros ingresos y egresos no operativos','Intereses/gasto financiero'],
                                            'is_sum':[1,1,-1]},
                    'Utilidad neta':{'dependencies':['Utilidad antes de impuestos','Impuestos de renta'],
                                            'is_sum':[1,-1]},
                    }
