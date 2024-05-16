import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { firstValueFrom } from 'rxjs';
import { Companie } from "src/app/Models/Companie";
import { environment } from "src/environments/environment";
import Swal from "sweetalert2";

@Injectable({
    providedIn: 'root',
})

export class CompanieService {

    constructor(private http: HttpClient) { }

    async create(objInfo: any) {

        try {

            const response = await firstValueFrom(this.http.post(environment.apiKey + 'companies', JSON.stringify(objInfo)))
            return response;


        } catch (error: any) {

            throw Swal.fire({
                title: '¡Lo sentimos!',
                icon: 'error',
                html: 'Ha habido un error al intentar depurar la información. Por favor, inténtelo de nuevo más tarde o comuniquese con el administrador  '
            })

        }

    }

    async getCompanie(nit: string): Promise<any> {

        try {

            const response = await firstValueFrom(this.http.get(environment.apiKey + 'companies/' + nit))
            return response;


        } catch (error: any) {

            throw Swal.fire({
                title: '¡Lo sentimos!',
                icon: 'error',
                html: 'Ha habido un error al intentar consultar la información. Por favor, inténtelo de nuevo más tarde o comuniquese con el administrador'
            })

        }

    }

    async getCompanieWithModel(nit: string) {

        try {

            const response = await firstValueFrom<Companie>(this.http.get<Companie>(environment.apiKey + 'companies?nit=' + nit))
            return response;


        } catch (error: any) {

            throw Swal.fire({
                title: '¡Lo sentimos!',
                icon: 'error',
                html: 'Ha habido un error al intentar consultar la información. Por favor, inténtelo de nuevo más tarde o comuniquese con el administrador'
            })

        }

    }


    async getHistoryValuation(nit: string): Promise<any> {

        try {

            return await firstValueFrom(this.http.get(environment.apiKey + 'pucs/client/data/' + nit))


        } catch (error: any) {

            throw Swal.fire({
                title: '¡Lo sentimos!',
                icon: 'error',
                html: 'Ha habido un error al intentar consultar la información. Por favor, inténtelo de nuevo más tarde o comuniquese con el administrador'
            })

        }

    }
}
