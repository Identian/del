import { Idle, DEFAULT_INTERRUPTSOURCES } from '@ng-idle/core';
import { Injectable } from '@angular/core';
import { SessionStorageService } from '../Storage/session.service';
import Swal from 'sweetalert2';

@Injectable({ providedIn: 'root' })

export class IdleService {

    private isSetUp: boolean = false;

    constructor(private idle: Idle, private sessionStorageService: SessionStorageService) {
        this.setup();
    }

    isServiceRunning() {
        return this.idle.isRunning();
    }

    startIdleSvc() {
        if (this.idle.isRunning()) {
            this.idle.stop();
        }

        this.idle.watch();
    }

    stopIdleSvc() {
        this.idle.stop();
    }

    private setup() {

        if (!this.isSetUp) {

            let idleStartSec: number = 900;
            let timeoutPeriodSec: number = 12;

            this.idle.setIdle(idleStartSec);

            this.idle.setTimeout(timeoutPeriodSec);

            this.idle.setInterrupts(DEFAULT_INTERRUPTSOURCES);

            this.idle.onIdleStart.subscribe(() => {
                this.idleSwal();
            });

            this.idle.onTimeout.subscribe(() => {
                this.sessionStorageService.clearData()
            });

            this.isSetUp = true;
        }
    }


    idleSwal() {
        let timerInterval: any;
        Swal.fire({
            title: 'Alerta de inactividad',
            html: 'Tu sesion esta a punto de expirar en <strong></strong> segundos, todo tu progreso se perdera.',
            timer: 10000,
            confirmButtonColor: '#198754',
            cancelButtonColor: '#d33',
            confirmButtonText: 'Continuar',
            timerProgressBar: true,
            didOpen: () => {
                timerInterval = setInterval(() => {
                    Swal!.getHtmlContainer()!.querySelector('strong')!.textContent = (
                        Swal.getTimerLeft()! / 1000
                    ).toFixed(0);
                }, 100);
            },
        }).then((response) => {
            if (response.isConfirmed) {
                clearInterval(timerInterval);
            } else {
                clearInterval(timerInterval);
                Swal.fire({
                    title: 'La sesión ha caducado',
                    text: 'Vuelve a iniciar sesión',
                    icon: 'info',
                    confirmButtonText: 'Aceptar',
                }).then((response) => {
                    if (response.isConfirmed || response.dismiss) {
                        window.location.reload()
                    }
                })
            }
        });
    }

}