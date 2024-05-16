import { Pipe, PipeTransform } from '@angular/core';

@Pipe({ name: 'numberPipe' })

export class NumberTransformPipe implements PipeTransform {

    transform(val: string | number) {

        if (typeof val == 'number') {
            val = String(val);
            return this.assingInput(val);
        } else {
            val = val.replaceAll('.', '');
            return this.onChangeInput(val);
        }
    }

    assingInput(val: any) {
        let newVal = val.split('.');

        if (newVal.length > 1) {
            let result = Number(newVal[0]);
            return result.toLocaleString('es-CO') + ',' + newVal[1]
        }
        else {
            let result = Number(newVal[0]);
            return result.toLocaleString('es-CO')
        }
    }

    onChangeInput(val: any) {
        if (val == '-') {
            return val;
        } else {
            let newVal = val.split(',');
            if (newVal.length > 1) {
                let result = Number(newVal[0]);
                return result.toLocaleString('es-CO') + ',' + newVal[1]
            } else {
                let result = Number(newVal[0]);
                return result.toLocaleString('es-CO');
            }
        }
    }
}