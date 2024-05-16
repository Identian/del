import { AuthenticationService } from "../Authentication/authentication.service";
import { environment } from "src/environments/environment";
import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Note } from "src/app/Common/components/notification/interfaces/Notes";
import { Observable, Subject, firstValueFrom } from 'rxjs';
import { SessionStorageService } from "../Storage/session.service";
import Swal from "sweetalert2";

@Injectable({
    providedIn: 'root',
})

export class OrchestratorService {

    private sharedToClasification = new Subject<number>();

    sharedToClasification$: Observable<number> = this.sharedToClasification.asObservable();

    private sharedToProfitLoss = new Subject<number>();

    sharedToProfitLoss$: Observable<number> = this.sharedToProfitLoss.asObservable();

    private sharedToCapex = new Subject<number>();

    sharedToCapex$: Observable<number> = this.sharedToCapex.asObservable();

    private sharedToCashFlow = new Subject<number>();

    sharedToCashFlow$: Observable<number> = this.sharedToCashFlow.asObservable();

    private sharedToValoration = new Subject<boolean>();

    sharedToValoration$: Observable<boolean> = this.sharedToValoration.asObservable();

    private sharedNotes = new Subject<Note[]>();

    sharedNotes$: Observable<Note[]> = this.sharedNotes.asObservable();

    constructor(private http: HttpClient, private authService: AuthenticationService,
        private sessionStorageService: SessionStorageService) { }

    async validateOrchestator(obj: any) {

        let user = this.authService.userInfo()?.username
        let url = environment.apiKey + 'orchestrator?' + 'date=' + obj.date + '&nit=' + obj.nit + '&periodicity=' + obj.periodicity + '&user=' + user
        let response = await firstValueFrom(this.http.get<any[]>(url))
        this.distributeToComponent(response)
    }

    distributeToComponent(component: any) {

        if (component.data_to_get.length > 0) {

            for (let index = 0; index < component.data_to_get.length; index++) {
                switch (component.data_to_get[index]) {
                    case 'CLASSIFICATION': {
                        this.setisClassification(component.id_assessment)
                        break
                    }
                    case 'PYG': {
                        this.setisProfitLoss(component.id_assessment)
                        break
                    }
                    case 'CAPEX': {
                        this.setisCapex(component.id_assessment)
                        break
                    }
                    case 'CASH_FLOW': {
                        this.setisCashFlow(component.id_assessment)
                        break
                    }
                    case 'VALORATION': {
                        this.setisValoration(true)
                        break
                    }
                }
            }
        }
        else {
            this.setisClassification(0)
        }
    }

    setisClassification(number: number) {
        this.sharedToClasification.next(number)
    }

    getisClassification() {
        return this.sharedToClasification$
    }

    setisProfitLoss(number: number) {
        this.sharedToProfitLoss.next(number);
    }

    getisProfitLoss() {
        return this.sharedToProfitLoss$
    }

    setisCapex(number: number) {
        this.sharedToCapex.next(number);
    }

    setNotes(notes: Note[]) {
        this.sharedNotes.next(notes);
    }

    getisCapex() {
        return this.sharedToCapex$
    }

    setisCashFlow(number: number) {
        this.sharedToCashFlow.next(number);
    }

    getisCashFlow() {
        return this.sharedToCashFlow$
    }

    setisValoration(status: boolean) {
        this.sharedToValoration.next(status);
    }

    getisValoration() {
        return this.sharedToValoration$
    }

    getNotes() {
        return this.sharedNotes$
    }

    async getIdAssessment(obj: any) {

        let user = this.authService.userInfo()?.username
        let url = environment.apiKey + 'orchestrator?' + 'date=' + obj.date + '&nit=' + obj.nit + '&periodicity=' + obj.periodicity + '&user=' + user
        return await firstValueFrom(this.http.get<any>(url))
    }

    async validateStatus(): Promise<boolean> {

        let status = await this.callStatus()

        if (status.status == 'TIMEOUT' || status.status == 'WORKING') {
            await this.sleep(500)
            await this.validateStatus();
        }
        else if (status.status == 'ERROR') {
            throw Swal.fire({
                title: '¡Lo sentimos!',
                icon: 'error',
                html: 'Ha ocurrido un error al consultar el estado, por favor comuniquese con el administrador'
            })
        }
        this.setNotes(status.notes)
        return true

    }

    async callStatus() {

        let idAssessment = JSON.parse(this.sessionStorageService.getData(environment.KEY_ID_ASSESSMENT)!)
        return await firstValueFrom(this.http.get<any>(environment.apiKey + 'orchestrator/status/' + idAssessment))

    }

    sleep(timeInMileSecond: number) {
        return new Promise(resolve => setTimeout(resolve, timeInMileSecond));
    }

}
