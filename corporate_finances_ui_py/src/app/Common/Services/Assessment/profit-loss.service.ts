import { environment } from "src/environments/environment";
import { firstValueFrom } from 'rxjs';
import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { IProfitAndLoss } from "src/app/Models/Profit-Loss";
import { IProjection } from "src/app/Models/Projections";
import Swal from "sweetalert2";

@Injectable({
    providedIn: 'root',
})

export class ProfitLossService {

    constructor(private http: HttpClient) { }

    async getProfitAndLossByAssesmentId(idAssessment: number) {

        return await firstValueFrom(this.http.get<IProfitAndLoss>(environment.apiKey + 'recovery/pyg/completed/' + idAssessment))

    }

    async recalculateProfitAndLoss(idAssessment: number) {

        return await firstValueFrom(this.http.get<IProfitAndLoss>(environment.apiKey + 'assessment/pyg/recalculate/' + idAssessment))

    }

    async calculateImplicitGrowth(obj: any): Promise<any> {

        try {

            const response = await firstValueFrom(this.http.post(environment.apiKey + 'assessment/pyg/implicit_growings', JSON.stringify(obj)));
            return response;

        } catch (error: any) {

            throw Swal.fire({
                title: '¡Lo sentimos!',
                icon: 'error',
                html: 'Ha habido un error al intentar obtener las columnas. Por favor, inténtelo de nuevo más tarde o comuniquese con el administrador '
            })

        }

    }

    async getProjectionByAssesmentId(idAssessment: number) {

        return await firstValueFrom(this.http.get<IProjection>(environment.apiKey + 'recovery/pyg/projections-only/' + idAssessment))
    }

    async saveProjections(obj: any): Promise<any> {

        try {

            const response = await firstValueFrom(this.http.post(environment.apiKey + 'assessment/pyg/projections-calculator', JSON.stringify(obj)));
            return response;

        } catch (error: any) {

            throw Swal.fire({
                title: '¡Lo sentimos!',
                icon: 'error',
                html: 'Ha habido un error al intentar obtener las columnas. Por favor, inténtelo de nuevo más tarde o comuniquese con el administrador '
            })

        }

    }

}
