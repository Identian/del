from decorators import handler_wrapper, timing, debugger_wrapper
import logging
import datetime
import sys


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class projection_calc_class(object):
    @debugger_wrapper('Error proyectando zeros', 'Error proyectando zeros')
    def pm_calculate_zero(self):
        return [0] * self.projection_dates_len
    
    @debugger_wrapper('Error proyectando constante', 'Error proyectando constante')
    def pm_calculate_constant(self, account_value):
        return [account_value] * self.projection_dates_len
    
    
    @debugger_wrapper('Error proyectando fijo', 'Error proyectando cuenta a tasa fija')
    def pm_calculate_fixed(self, account_value, fixed_percentages):
        projections = [account_value * (1 + fixed_percentages[0]/100)]
        for i in range(1, self.projection_dates_len):
            projections.append(projections[i - 1] * (1 + fixed_percentages[i]/100))
        return projections
    

    @debugger_wrapper('Error generando proyecciones de proporcion', 'Error proyectando en proporción')
    def pm_calculate_proportion(self, account_value, vs_vector):
        projections = [account_value]
        #vs_vector.insert(0, 0)
        
        for i in range(1, self.projection_dates_len + 1):
            try:
                logger.info(f'[esta intentando] A: {projections[i - 1]} B: {vs_vector[i - 1]} C: {vs_vector[i]}')
                projections.append(projections[i - 1] / vs_vector[i - 1] * vs_vector[i])
            except Exception as e:
                logger.error(f'[mira aca] marcó el sgte error: {str(e)}')
                projections.append(0)
 
        projections.pop(0)
        #vs_vector.pop(0) #Estos pop son necesarios porque el vector de entrada fue modificado y a la función entraron referenciados, no copiados
        return projections


    @debugger_wrapper('Error generando proyecciones impositvas', 'Error calculando proyecciones impositvas')
    def pm_calculate_impositive(self, atributes, vs_vector):
        projections = []
        for i in range(self.projection_dates_len):
            projections.append(vs_vector[i] * float(atributes[i])/100)
        return projections
        
    
