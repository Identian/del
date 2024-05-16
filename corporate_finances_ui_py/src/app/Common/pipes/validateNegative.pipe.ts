import { DecimalPipe } from '@angular/common';
import { Pipe, PipeTransform } from '@angular/core';

@Pipe({ name: 'validateNegative' })

export class ValidateNegativePipe implements PipeTransform {

    transform(val: number): string | number {

        if (val == undefined) {
            return 'Por Calcular'
        }
        else {
            let decimalPipe = new DecimalPipe('es')
            return decimalPipe.transform(val, '1.2-2')!
        }

    }
}