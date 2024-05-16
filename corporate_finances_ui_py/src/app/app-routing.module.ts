import { AuthGuard } from './Guards/auth.guard';
import { CompanieCreateComponent } from './Views/Companies/companie-create/companie-create.component';
import { LoginComponent } from './Views/Authentication/login/login.component';
import { NgModule } from '@angular/core';
import { NotFoundComponent } from './Common/components/not-found/not-found.component';
import { PanelComponent } from './Views/Assessment/panel/panel.component';
import { ReportSearchComponent } from './Views/Reports/report-search/report-search.component';
import { RouterModule, Routes } from '@angular/router';

const routes: Routes = [
  { path: '', redirectTo: 'login', pathMatch: 'full' },
  { path: 'login', component: LoginComponent },
  { path: 'assessment', component: PanelComponent, canActivate: [AuthGuard] },
  { path: 'report-search', component: ReportSearchComponent, canActivate: [AuthGuard] },
  { path: 'companie-create', component: CompanieCreateComponent, canActivate: [AuthGuard] },
  { path: '**', component: NotFoundComponent }

];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
