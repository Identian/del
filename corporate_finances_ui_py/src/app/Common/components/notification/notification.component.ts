import { Component, OnInit } from '@angular/core';
import { Note } from 'src/app/Common/components/notification/interfaces/Notes';
import { OrchestratorService } from '../../Services/Assessment/orchestrator.service';
import Swal from 'sweetalert2';

@Component({
  selector: 'app-notification',
  templateUrl: './notification.component.html',
  styleUrls: ['./notification.component.css']
})
export class NotificationComponent implements OnInit {

  notes: Note[] = []

  totalNotification = 0

  constructor(private orchestratorService: OrchestratorService) { }

  ngOnInit(): void {

    this.orchestratorService.getNotes().subscribe(notes => {

      if (notes != undefined) {
        this.notes = notes
        this.totalNotification = notes.length
      }

    })
  }

  showNotification() {

    let html = ""


    if (this.notes.length > 0) {

      for (var value of this.notes) {

        html += "<h2>" + value.context + "</h2>" + "<br>"
        html += '<ul>'

        for (var note of value.notes) {

          html += '<li>' + note + '</li>'

        }

        html += '</ul>'
      }

    }
    else {

      html = "No se encontraron notas"

    }

    Swal.fire({
      icon: 'info',
      html: html,
      width: '850px',
      heightAuto: true
    })

  }


}
