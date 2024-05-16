import { Component } from '@angular/core';
import Swal from 'sweetalert2';
import { MsalService } from '@azure/msal-angular';
import { AuthenticationService } from '../../Services/Authentication/authentication.service';

@Component({
  selector: 'app-header',
  templateUrl: './header.component.html'
})
export class HeaderComponent {

  constructor(private authenticationService: AuthenticationService) {

  }

  getName() {
    return this.authenticationService.userInfo()?.username
  }

  LogOut(): void {

    Swal.fire({
      title: '¿Estás seguro de que quieres cerrar la sesión en Delta?',
      icon: 'warning',
      showCancelButton: true,
      confirmButtonColor: '#198754',
      cancelButtonColor: '#d33',
      confirmButtonText: 'Si',
      cancelButtonText: 'Cancelar'
    }).then((result) => {
      if (result.isConfirmed) {
        this.authenticationService.logOut()
      }
    })
  }
}
