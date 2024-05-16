import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { firstValueFrom } from 'rxjs';
import { environment } from "src/environments/environment";
import { IExport } from "../interface/Export";
import Swal from "sweetalert2";

@Injectable({
    providedIn: 'root',
})

export class ReportService {

    constructor(private http: HttpClient) { }

    async download(obj: IExport) {

        try {

            const response = await firstValueFrom(this.http.get<any>(environment.apiKey + 'report?' + 'date=' + obj.date + '&' + 'nit=' + obj.nit + '&' + 'periodicity=' + obj.periodicity + '&' + 'user=' + obj.user))
            return response;


        } catch (error: any) {

            throw Swal.fire({
                title: '¡Lo sentimos!',
                icon: 'error',
                html: 'Ha habido un error al intentar depurar la información. Por favor, inténtelo de nuevo más tarde o comuniquese con el administrador  '
            })

        }

    }
}
