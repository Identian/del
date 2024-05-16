import { Injectable, OnInit } from "@angular/core";
import { Router } from "@angular/router";
import { MsalService } from "@azure/msal-angular";
import { AuthenticationResult } from "@azure/msal-browser";
import { environment } from "src/environments/environment";
import { SessionStorageService } from "../Storage/session.service";

const loginRequest = {
    scopes: ["User.Read"],
}

@Injectable({
    providedIn: 'root'
})

export class AuthenticationService {

    constructor(private msalService: MsalService, private sessionStorageService: SessionStorageService, private router: Router) {

    }

    logIn() {

        this.msalService.loginPopup(loginRequest).subscribe((response: AuthenticationResult) => {
            this.msalService.instance.setActiveAccount(response.account)
            this.msalService.handleRedirectObservable().subscribe(() => {
                const silentRequest = {
                    scopes: [environment.azure.cliendId + "/.default"],
                    account: this.msalService.instance.getActiveAccount()!
                }

                this.msalService.instance.acquireTokenSilent(silentRequest).then(response => {
                    this.sessionStorageService.saveData(environment.KEY_TOKEN, JSON.stringify(response))
                    this.router.navigate(['assessment'])
                })
            })
        })
    }

    logOut() {
        this.msalService.logoutPopup({
            mainWindowRedirectUri: "/login"
        });
    }

    isLoggedIn(): boolean {
        return this.msalService.instance.getActiveAccount() != null
    }

    userInfo() {
        return this.msalService.instance.getActiveAccount()
    }

    validateTokenExp() {

        const tokenExpirateOn = JSON.parse(this.sessionStorageService.getData(environment.KEY_TOKEN)!)
        const forceRefresh = (new Date(tokenExpirateOn.expiresOn) < new Date());
        if (forceRefresh) {
            return true
        }
        return false
    }

    async refreshToken() {

        const silentRequest = {
            scopes: [environment.azure.cliendId + "/.default"],
            account: this.msalService.instance.getActiveAccount()!
        }

        await this.msalService.instance.acquireTokenSilent(silentRequest).then(response => {
            this.sessionStorageService.saveData(environment.KEY_TOKEN, JSON.stringify(response))
        })

    }

}