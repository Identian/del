import { Component, OnInit } from '@angular/core';
import { Router, NavigationStart, RouterEvent } from '@angular/router';
import { filter } from 'rxjs';
import { IdleService } from './Common/Services/Idle/Idle.service';
import { AuthenticationService } from './Common/Services/Authentication/authentication.service';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {

  showHead: boolean = false;

  constructor(private router: Router, private idleService: IdleService, private authenticationService: AuthenticationService) {

    this.validateHead()
    this.validateIdle()

  }
  
  validateIdle() {
    this.router.events.pipe(filter(e => e instanceof RouterEvent)).subscribe(e => {

      let isLoggedIn = this.authenticationService.isLoggedIn()
      if (isLoggedIn != false) {
        if (!this.idleService.isServiceRunning()) {
          this.idleService.startIdleSvc();
        }
      } else {
        if (this.idleService.isServiceRunning()) {
          this.idleService.stopIdleSvc();
        }
      }
    })
  }

  validateHead() {
    this.router.events.forEach((event) => {
      if (event instanceof NavigationStart) {
        if (event['url'] == '/login' || event['url'] == '/recover-password' || event['url'] == '/' || event['url'] == '**') {
          this.showHead = false;
        } else {
          this.showHead = true;
        }
      }
    });
  }

}
