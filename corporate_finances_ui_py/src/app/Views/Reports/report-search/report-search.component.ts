import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { CompanieService } from 'src/app/Views/Companies/services/companie.service';
import { ReportService } from 'src/app/Views/Reports/services/report.service';
import { Companie } from 'src/app/Models/Companie';
import Swal from 'sweetalert2';
import { IExport } from '../interface/Export';

@Component({
  selector: 'app-report-search',
  templateUrl: './report-search.component.html',
})
export class ReportSearchComponent {

  reportForm: FormGroup;
  periodicityOption: string[] = []
  email: string[] = []
  datesOption!: Companie | undefined

  constructor(private formBuilder: FormBuilder, private companieService: CompanieService, private reportService: ReportService) {

    this.reportForm = this.formBuilder.group({
      nit: new FormControl('', Validators.compose([Validators.required, Validators.minLength(9)])),
      digitVerification: new FormControl('', Validators.compose([Validators.required])),
      name: new FormControl({ value: '', disabled: true }),
      sector: new FormControl({ value: '', disabled: true }),
      date: new FormControl({ value: null, disabled: true }, Validators.required),
      periodicity: new FormControl({ value: null, disabled: true }, Validators.required),
      email: new FormControl({ value: null, disabled: true }, Validators.required)
    })
  }

  getValorationsByCompanie() {

    let nit = this.reportForm.get('nit')?.value + '-' + this.reportForm.get('digitVerification')?.value;

    this.companieService.getCompanieWithModel(nit).then(response => {
      response.data = response.data.filter(model => model.user != null)
      this.datesOption = response
      this.reportForm.get('name')?.setValue(response.company.name)
      this.reportForm.get('sector')?.setValue(response.company.sector)
      this.reportForm.get('date')?.enable()
    })
  }

  filterPeriodicity() {
    this.periodicityOption = []
    this.email = []
    this.periodicityOption.push(this.datesOption!.data.find(model => model.date == this.reportForm.get('date')!.value)!.periodicity)
    this.email.push(this.datesOption!.data.find(model => model.date == this.reportForm.get('date')!.value)!.user)
    this.reportForm.get('periodicity')?.enable()
    this.reportForm.get('email')?.enable()
  }

  export() {

    let obj: IExport = {
      date: this.reportForm.get('date')?.value,
      nit: this.reportForm.get('nit')?.value + '-' + this.reportForm.get('digitVerification')?.value,
      periodicity: this.reportForm.get('periodicity')?.value,
      user: this.reportForm.get('email')?.value
    }

    this.reportService.download(obj).then(response => {
      if (response != undefined) {
        window.open(response.download_url)
      }
      else {
        Swal.fire({
          title: 'Lo sentimos',
          icon: 'info',
          text: 'No se encontro información para descargar, valide con el administrador'
        })
      }
    })

  }


}

