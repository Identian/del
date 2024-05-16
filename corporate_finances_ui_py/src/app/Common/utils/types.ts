export type GroupSelect = 'FixedAssets' | 'Period' | 'Acumulated'
export type MethodProjectionAssests = 'D&A manual' | 'D&A en línea recta'
export type MethodProjectionCapexDepreactionAmortization = 'Porcentaje de otra variable' | 'Manual' | ''
export type MethodProjectionGeneral = 'Tasa de crecimiento fija' | 'Porcentaje de otra variable' | 'Valor constante' | 'Tasas impositivas' | 'Input' | 'Cero' | 'PXQ'
export type Period = 'history' | 'projection' | 'projections'
export type ModalContext = {
    'PYG': 'PYG'
    'WORKING CAPITAL': 'WK'
    'PATRIMONY': 'PATRIMONY'
    'OTHER PROJECTION': 'OTHER PROJECTION'
}