import { Pipe, PipeTransform } from '@angular/core';

@Pipe({ name: 'numberType' })

export class NumberType implements PipeTransform {
    transform(val: string | number) {

        if (typeof val != 'string') {
            return val
        }
        else {
            return Number(val.replaceAll('.', '').replaceAll(',', '.'))
        }
    }
}