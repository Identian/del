import { Component } from '@angular/core';
import { AuthenticationService } from 'src/app/Common/Services/Authentication/authentication.service';

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html'
})

export class LoginComponent {

  constructor(private authenticationService: AuthenticationService) {

  }

  logIn() {
    this.authenticationService.logIn()
  }

}
