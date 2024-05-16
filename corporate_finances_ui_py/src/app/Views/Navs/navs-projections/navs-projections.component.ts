import { Component, Input, OnInit } from '@angular/core';
import { FormArray, FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { ProfitLossService } from 'src/app/Common/Services/Assessment/profit-loss.service';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { IProjection, IProjectionData } from 'src/app/Models/Projections';
import { environment } from 'src/environments/environment';
import { SessionStorageService } from 'src/app/Common/Services/Storage/session.service';
import { convertDate } from 'src/app/Common/Utils/tableUtil';
import { ConstantsDelta } from 'src/app/Common/utils/constants';
import Swal from 'sweetalert2';
import { MethodProjectionGeneral } from 'src/app/Common/utils/types';

@Component({
  selector: 'app-navs-projections',
  templateUrl: './navs-projections.component.html',
  styleUrls: ['./navs-projections.component.css']
})

export class NavsProjectionsComponent implements OnInit {

  @Input() accountsComplete: string[] = [];
  projectionMethods: MethodProjectionGeneral[] = ConstantsDelta.methodsProjectionGeneral
  projectionForm: FormGroup;
  isRecovery = false;
  isAnualize = false;

  constructor(private formBuilder: FormBuilder, private profitLossService: ProfitLossService,
    private modalService: NgbActiveModal, private sessionStorageService: SessionStorageService) {

    this.projectionForm = new FormGroup({
      id_assessment: new FormControl('', Validators.required),
      year: new FormControl('', Validators.required),
      datesHistory: new FormArray([], Validators.required),
      datesProjections: new FormArray([], Validators.required),
      projection_data: new FormArray([], Validators.required),
    });

  }

  ngOnInit(): void {

    let idAssessment = Number(this.sessionStorageService.getData(environment.KEY_ID_ASSESSMENT)!)
    this.profitLossService.getProjectionByAssesmentId(idAssessment).then(projection => {

      if (projection.datesProjections.length > 0) {
        this.isRecovery = true;
        this.assignValues(projection)
      }
      else {
        this.assigvaluesFirstTime(projection)
      }
    })

  }

  getDatesHistory(): FormArray {
    return this.projectionForm.get('datesHistory') as FormArray;
  }

  getDatesProjection(): FormArray {
    return this.projectionForm.get('datesProjections') as FormArray;
  }

  getProjections(): FormArray {
    return this.projectionForm.get('projection_data') as FormArray;
  }

  getAtributesProjection(empIndex: number, period: string): FormArray {
    return this.getProjections().at(empIndex).get('atributes')?.get(period) as FormArray;
  }

  save() {

    Swal.fire({
      title: '¿Estas seguro de guardar?',
      icon: 'info',
      showCancelButton: true,
      confirmButtonColor: '#198754',
      cancelButtonColor: '#d33',
      confirmButtonText: 'Si',
      cancelButtonText: 'Cancelar'
    }).then((result) => {
      if (result.isConfirmed) {

        this.profitLossService.saveProjections(this.projectionForm.value).then(() => {
          Swal.fire({
            title: 'Guardado',
            text: 'La información se ha guardado correctamente',
            icon: 'success',
            confirmButtonText: 'OK'
          }).then(response => {
            if (response.isConfirmed) {
              this.modalService.close();
            }
          })

        })

      }
    })
  }

  assignValues(projectionData: IProjection) {

    this.projectionForm.get('year')?.setValue(projectionData.year)

    this.projectionForm.get('id_assessment')?.setValue(projectionData.id_assessment)

    projectionData.datesHistory.map(item => this.getDatesHistory().push(new FormControl(item)))

    projectionData.datesProjections.map((item) => this.getDatesProjection().push(new FormControl(item)))

    projectionData.projection_data.map((item) => this.getProjections().push(this.create(item)))

  }

  assigvaluesFirstTime(projectionData: IProjection) {

    this.projectionForm.get('year')?.setValue('')

    this.projectionForm.get('id_assessment')?.setValue(projectionData.id_assessment)

    projectionData.datesHistory.map(item => this.getDatesHistory().push(new FormControl(item)))

    projectionData.projection_data.forEach((item) => this.getProjections().push(this.createFirstTime(item)))

  }

  create(item: IProjectionData) {
    return this.formBuilder.group({
      account: new FormControl(item.account),
      accountProjector: new FormControl(item.accountProjector == 'No aplica' ? 'Seleccione' : item.accountProjector, Validators.required),
      method: new FormControl(item.method, Validators.required),
      atributes: new FormGroup({
        history: new FormArray(item.atributes.history.map((value: number) => new FormControl(value))),
        projection: new FormArray(item.atributes.projection.map((value: number) => new FormControl(value)))
      }),
      explication: new FormControl(item.explication, Validators.required),
    });
  }

  createFirstTime(item: IProjectionData) {
    return this.formBuilder.group({
      account: new FormControl(item.account),
      accountProjector: new FormControl('Seleccione', Validators.required),
      method: new FormControl('Seleccione', Validators.required),
      atributes: new FormGroup({
        history: new FormArray(item.atributes.history.map((value: number) => new FormControl(value))),
        projection: new FormArray([])
      }),
      explication: new FormControl('', Validators.required),
    });
  }

  addDateProjections() {

    if (this.projectionForm.get('year')?.value != '') {

      this.getDatesProjection().clear();

      let companieInformation = JSON.parse(this.sessionStorageService.getData(environment.KEY_COMPANIE_INFORMATION)!)
      let dateCompanie = convertDate(companieInformation.date)

      if (dateCompanie.getMonth() < 11) {
        this.isAnualize = true
        this.getDatesProjection().push(new FormControl('Diciembre ' + dateCompanie.getFullYear().toString()))
      }

      for (let index = 1; index <= this.projectionForm.get('year')?.value; index++) {
        this.getDatesProjection().push(new FormControl(String(dateCompanie.getFullYear() + index)))
      }

      this.addYearAtributes()
    }

  }

  addYearAtributes() {

    let projections = this.getProjections()
    let temProjectionValue = projections.value
    let yearProjection = this.projectionForm.get('year')?.value

    for (let index = 0; index < projections.length; index++) {

      let atributesByAccount = projections.at(index).get('atributes')?.get('projection') as FormArray
      atributesByAccount.clear();

      let addYearProjection = this.isAnualize ? yearProjection + 1 : yearProjection

      for (let index2 = 0; index2 < addYearProjection; index2++) {

        if (this.isRecovery) {
          let numbers = temProjectionValue[index].atributes.projection;
          let isAssingValidator = projections.at(index).get('method')?.value == 'Tasa de crecimiento fija' || projections.at(index).get('method')?.value == 'Tasas impositivas' || projections.at(index).get('method')?.value == 'Input';
          atributesByAccount.push(new FormControl((numbers[index2] == '' || numbers[index2] == undefined) ? '' : Number(numbers[index2]), isAssingValidator ? Validators.required : null));
        }
        else {
          atributesByAccount.push(new FormControl(''))
        }
      }
    }

  }

  helpEquation(): void {
    Swal.fire({
      title: 'Tipos de proyección',
      icon: 'question',
      html: '<ul><li><strong>Tasa de crecimiento fija:</strong> Este método calcula el valor futuro de una línea de negocio a una tasa dada por el usuario:<br><div><br><p>x = x <sub>t-1</sub> * (1 + i%)</p><p><i>donde x es el valor en el año proyectado.</i></p><p><i>donde x <sub>t-1</sub> es el valor a proyectar un periodo antes del año de proyección.</i></p><p><i>i% la tasa dada para los años de proyección.</i></p></div></li><li><strong>Porcentaje de otra variable:</strong> Este método calcula el valor futuro de una línea de negocio basado en su porcentaje de participación sobre otra línea principal de negocio (Ejemplo: ingresos):<br><div><br><p>x = x <sub>t-1</sub> / y <sub>t-1</sub> * y</p><p><i>donde x es el valor en el año proyectado.</i></p><p><i>x <sub>t-1</sub> es el valor a proyectar un periodo antes del año de proyección.</i></p><p><i>y <sub>t-1</sub> es el valor de la cuenta de la proporción un periodo antes del año de proyección.</i></p><p><i>y es el valor de la cuenta de la proporción en el año de proyección.</i></p></div></li><li><strong>Valor constante:</strong> Esta opción no aplica ningún método de proyección y deja el mismo valor para cada año de proyección.</li><br><li><strong>Tasas impositivas:</strong> Este método calcula el valor futuro de una línea de negocio basado en un porcentaje sobre otra línea de negocio:<br><div><br><p>x = y * i%</p><p><i>donde x es el valor en el año proyectado.</i></p><p><i>y es la linea de negocio sobre la que aplique la tasa.</i></p><p><i>i% la tasa dada a los años de proyección</i></p></div></li></ul>'
    })
  }


}
