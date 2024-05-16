import { AuthenticationService } from "../Authentication/authentication.service";
import { environment } from "src/environments/environment";
import { finalize, from, lastValueFrom, Observable } from "rxjs";
import { HttpEvent, HttpHandler, HttpInterceptor, HttpRequest } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { SessionStorageService } from "../Storage/session.service";
import { SpinnerService } from "./spinner.service";

@Injectable({
    providedIn: 'root'
})

export class InterceptorService implements HttpInterceptor {

    constructor(private spinnerService: SpinnerService, private authenticationService: AuthenticationService,
        private sessionStorageService: SessionStorageService) {

    }

    intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {

        return from(this.handle(req, next))

    }

    async handle(req: HttpRequest<any>, next: HttpHandler) {

        this.spinnerService.show();

        if (this.authenticationService.validateTokenExp()) {

            await this.authenticationService.refreshToken()

            let token = JSON.parse(this.sessionStorageService.getData(environment.KEY_TOKEN)!)

            req = req.clone({
                headers: req.headers.set('Authorization', `${token.accessToken}`)
            });

            return await lastValueFrom(next.handle(req).pipe(
                finalize(() => {
                    this.spinnerService.hide()
                })
            ))
        }

        let token = JSON.parse(this.sessionStorageService.getData(environment.KEY_TOKEN)!)

        req = req.clone({
            headers: req.headers.set('Authorization', `${token.accessToken}`)
        });

        return await lastValueFrom(next.handle(req).pipe(
            finalize(() => {
                this.spinnerService.hide()
            })
        ))

    }

}