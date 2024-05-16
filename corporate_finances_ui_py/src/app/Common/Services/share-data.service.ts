import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { IProjection } from 'src/app/Models/Resume';
import { AuthenticationService } from './Authentication/authentication.service';

@Injectable({
  providedIn: 'root'
})
export class ShareDataService {

  private sharedDataPatrimony: Subject<IProjection> = new Subject<IProjection>();

  sharedDataPatrimony$: Observable<IProjection> = this.sharedDataPatrimony.asObservable();

  private sharedDataWorkingCapital: Subject<IProjection> = new Subject<IProjection>();

  sharedDataWorkingCapital$: Observable<IProjection> = this.sharedDataWorkingCapital.asObservable();

  private sharedDataOtherProjections: Subject<IProjection> = new Subject<IProjection>();

  sharedDataOtherProjections$: Observable<IProjection> = this.sharedDataOtherProjections.asObservable();

  private sharedDataCashFlow: Subject<any> = new Subject<any>();

  sharedDataCashFlow$: Observable<any> = this.sharedDataCashFlow.asObservable();

  private sharedDataCapexToCashFlow: Subject<any> = new Subject<any>();

  sharedDataCapexToCashFlow$: Observable<any> = this.sharedDataCapexToCashFlow.asObservable();

  private sharedDataCapexToPyg: Subject<any> = new Subject<any>();

  sharedDataCapexToPyg$: Observable<any> = this.sharedDataCapexToPyg.asObservable();

  private sharedDataArchive: any

  private sharedDataProjection: any

  private sharedDataProjectionHistory: any

  private projectionsModals: any

  private sharedDataListAccountPyg: any

  private yearsProjecction: any

  private DataCashFlow: any

  private calculateProjection: any

  private sharedDataExistinProjection: Subject<any> = new Subject<any>();

  sharedDataExistinProjection$: Observable<any> = this.sharedDataExistinProjection.asObservable();

  private sharedDataFutureProjection: Subject<any> = new Subject<any>();

  sharedDataFutureProjection$: Observable<any> = this.sharedDataFutureProjection.asObservable();

  private companyInformation: any

  private existingProjection: any

  private futureProjection: any

  private sharedDataValuation: Subject<any> = new Subject<any>();

  sharedDataValuation$: Observable<any> = this.sharedDataValuation.asObservable();

  private sharedDataCashFlowToPyg: Subject<any> = new Subject<any>();

  sharedDataCashFlowToPyg$: Observable<any> = this.sharedDataCashFlowToPyg.asObservable();

  private sharedUtilityPygToCashFlow: Subject<any> = new Subject<any>();

  sharedUtilityPygToCashFlow$: Observable<any> = this.sharedUtilityPygToCashFlow.asObservable();

  constructor(private authenticationService: AuthenticationService) {

  }

  setPatrimony(projection: any) {
    this.sharedDataPatrimony.next(projection);
  }

  setWorkingCapital(projection: any) {
    this.sharedDataWorkingCapital.next(projection)
  }

  setCapex(capex: any) {
    this.sharedDataCapexToCashFlow.next(capex)
  }

  setCapexToPyg(capex: any) {
    this.sharedDataCapexToPyg.next(capex)
  }

  setValuation(valuationList: any) {
    this.sharedDataValuation.next(valuationList)
  }

  setOtherProjection(projection: any) {
    this.sharedDataOtherProjections.next(projection)
  }

  setOtherDataCashFlow(projection: any) {
    this.sharedDataCashFlow.next(projection)
  }

  setCompanyInformation(obj: any) {
    this.companyInformation = obj
  }

  setCashFlowToPyg(obj: any) {
    this.sharedDataCashFlowToPyg.next(obj)
  }

  setUtilityPygToCashFlow(obj: any) {
    this.sharedUtilityPygToCashFlow.next(obj)
  }

  getCompanyInformation() {
    return this.companyInformation
  }

  setAccountPyg(obj: any) {
    this.sharedDataListAccountPyg = obj
  }

  setDataCashFlow(obj: any) {
    this.DataCashFlow = obj
  }

  getDataCashFlow() {
    return this.DataCashFlow
  }

  setProjectionModal(obj: any) {
    this.projectionsModals = obj
  }

  getAccountPyg() {
    return this.sharedDataListAccountPyg
  }

  setArchive(obj: any) {
    this.sharedDataArchive = obj
  }

  getArchive() {
    return this.sharedDataArchive
  }

  setProjection(obj: any) {
    this.sharedDataProjection = obj
  }

  getProjection() {
    return this.sharedDataProjection
  }

  setHistoryProjection(obj: any) {
    this.sharedDataProjectionHistory = obj
  }

  getHistoryProjection() {
    return this.sharedDataProjectionHistory
  }

  setCalculateProjection(obj: any) {
    this.calculateProjection = obj
  }

  getCalculateProjection() {
    return this.calculateProjection
  }

  setExistinProjection(projectionExisting: any) {
    this.sharedDataExistinProjection.next(projectionExisting)
  }

  setFutureProjection(projectionExisting: any) {
    this.sharedDataFutureProjection.next(projectionExisting)
  }

  setCashExistingProjection(listExistingProjection: any) {
    this.existingProjection = listExistingProjection
  }

  getCashExistingProjection() {
    return this.existingProjection
  }

  setCashFutureProjections(listExistingProjection: any) {
    this.futureProjection = listExistingProjection
  }

  getCashFutureProjections() {
    return this.futureProjection
  }

}
