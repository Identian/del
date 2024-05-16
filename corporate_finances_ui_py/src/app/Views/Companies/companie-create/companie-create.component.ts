import { Component } from '@angular/core';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { CompanieService } from 'src/app/Views/Companies/services/companie.service';
import Swal from 'sweetalert2';

@Component({
  selector: 'app-companie-create',
  templateUrl: './companie-create.component.html'
})

export class CompanieCreateComponent {

  companieForm: FormGroup;

  constructor(private formBuilder: FormBuilder, private companieService: CompanieService) {
    this.companieForm = this.formBuilder.group({
      nit: new FormControl('', Validators.compose([Validators.required, Validators.minLength(9)])),
      digitVerification: new FormControl('', Validators.compose([Validators.required])),
      name: new FormControl('', Validators.compose([Validators.required])),
      sector: new FormControl(null, Validators.compose([Validators.required]))
    })

  }

  create() {

    const obj = {
      nit: this.companieForm.get('nit')?.value + '-' + this.companieForm.get('digitVerification')?.value,
      name: this.companieForm.get('name')?.value,
      sector: this.companieForm.get('sector')?.value
    }
    this.companieService.create(obj).then(response => {
      if (response != null) {
        Swal.fire({
          title: 'Excelente',
          text: 'La empresa se ha creado correctamente.',
          icon: 'success'
        })
      }
    })
  }

  validateCompanie() {

    this.companieService.getCompanie(this.companieForm.get('nit')?.value + '-' + this.companieForm.get('digitVerification')?.value)
      .then(companie => {
        if (companie.hasOwnProperty('NIT')) {
          Swal.fire({
            title: 'Lo sentimos',
            text: 'El nit ingresado ya ha sido registrado.',
            icon: 'warning'
          })
          this.companieForm.get('nit')?.setValue('');
          this.companieForm.get('digitVerification')?.setValue('');
        }
      })
  }

}
