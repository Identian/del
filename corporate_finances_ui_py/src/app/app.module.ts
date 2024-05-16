import { NgModule, CUSTOM_ELEMENTS_SCHEMA, LOCALE_ID } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { HttpClientModule, HTTP_INTERCEPTORS } from '@angular/common/http';
import { LoginComponent } from './Views/Authentication/login/login.component';
import { PanelComponent } from './Views/Assessment/panel/panel.component';
import { HeaderComponent } from './Common/pages/header/header.component';
import { FooterComponent } from './Common/pages/footer/footer.component';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { PvPreviewExcelComponent } from './Views/Assessment/beginning/modals/pv-preview-excel/pv-preview-excel.component';
import { BeginningComponent } from './Views/Assessment/beginning/beginning.component';
import { NgxBootstrapMultiselectModule } from 'ngx-bootstrap-multiselect';
import { PvInitialSubaccountsComponent } from './Views/Assessment/beginning/modals/pv-initial-subaccounts/pv-initial-subaccounts.component';
import { PvAccountCreationComponent } from './Views/Assessment/beginning/modals/pv-account-creation/pv-account-creation.component';
import { PvFinalSubaccountsComponent } from './Views/Assessment/beginning/modals/pv-final-subaccounts/pv-final-subaccounts.component';
import { NgxSpinnerModule } from 'ngx-spinner';
import { InterceptorService } from './Common/Services/Spinner/interceptor.service';
import { ClassificationComponent } from './Views/Assessment/classification/classification.component';
import { PvClassificationSubaccountsComponent } from './Views/Assessment/classification/modals/pv-classification-subaccounts/pv-classification-subaccounts.component';
import { PvSubaccountsChildComponent } from './Views/Assessment/classification/modals/pv-subaccounts-child/pv-subaccounts-child.component';
import { CompanieCreateComponent } from './Views/Companies/companie-create/companie-create.component';
import { ProfitLossComponent } from './Views/Assessment/profit-loss/profit-loss.component';
import { ProfitLossService } from './Common/Services/Assessment/profit-loss.service';
import { registerLocaleData } from '@angular/common';
import localeEs from '@angular/common/locales/es';
import { PvProjectionsComponent } from './Views/Assessment/profit-loss/modals/pv-projections/pv-projections.component';
import { NotFoundComponent } from './Common/components/not-found/not-found.component';
import { PvDatePucsComponent } from './Views/Assessment/classification/modals/pv-date-pucs/pv-date-pucs.component';
import { NavsImplicitGrowthComponent } from './Views/Navs/navs-implicit-growth/navs-implicit-growth.component';
import { NavsProjectionsComponent } from './Views/Navs/navs-projections/navs-projections.component';
import { CashFlowComponent } from './Views/Assessment/cash-flow/cash-flow.component';
import { PvWorkingCapitalComponent } from './Views/Assessment/cash-flow/modals/pv-working-capital/pv-working-capital.component';
import { PvPatrimonyComponent } from './Views/Assessment/cash-flow/modals/pv-patrimony/pv-patrimony.component';
import { PvOtherProjectionsComponent } from './Views/Assessment/cash-flow/modals/pv-other-projections/pv-other-projections.component';
import { PvFinancialDebtComponent } from './Views/Assessment/cash-flow/modals/pv-financial-debt/pv-financial-debt.component';
import { CapexComponent } from './Views/Assessment/capex/capex.component';
import { ValuationComponent } from './Views/Assessment/valuation/valuation.component';
import { NumberTransformPipe } from './Common/pipes/number.pipe';
import { NgIdleKeepaliveModule } from '@ng-idle/keepalive';
import { IPublicClientApplication, PublicClientApplication } from '@azure/msal-browser';
import { MSAL_INSTANCE, MsalService } from '@azure/msal-angular';
import { environment } from 'src/environments/environment';
import { PvAssetDepreciationComponent } from './Views/Assessment/capex/modals/pv-asset-depreciation/pv-asset-depreciation.component';
import { PvDepreciationNewCapexComponent } from './Views/Assessment/capex/modals/pv-depreciation-new-capex/pv-depreciation-new-capex.component';
import { ReportSearchComponent } from './Views/Reports/report-search/report-search.component';
import { PvParametricModelComponent } from './Views/Assessment/beginning/modals/pv-parametric-model/pv-parametric-model.component';
import { PvInitValorationComponent } from './Views/Assessment/beginning/modals/pv-init-valoration/pv-init-valoration.component';
import { NotificationComponent } from './Common/components/notification/notification.component';
import { ValidateNegativePipe } from './Common/pipes/validateNegative.pipe';

const isIE = window.navigator.userAgent.indexOf('MSIE ') > -1 || window.navigator.userAgent.indexOf('Trident/') > -1;

export function MSLAInstanceFactory(): IPublicClientApplication {
  return new PublicClientApplication({
    auth: {
      clientId: environment.azure.cliendId,
      authority: environment.azure.authority,
    },
    cache: {
      cacheLocation: 'sessionStorage',
      storeAuthStateInCookie: isIE,
    }
  })
}

@NgModule({
  declarations: [
    AppComponent,
    LoginComponent,
    PanelComponent,
    HeaderComponent,
    FooterComponent,
    PvPreviewExcelComponent,
    BeginningComponent,
    PvInitialSubaccountsComponent,
    PvAccountCreationComponent,
    PvFinalSubaccountsComponent,
    ClassificationComponent,
    PvClassificationSubaccountsComponent,
    PvSubaccountsChildComponent,
    CompanieCreateComponent,
    ProfitLossComponent,
    PvProjectionsComponent,
    NotFoundComponent,
    PvDatePucsComponent,
    NavsImplicitGrowthComponent,
    NavsProjectionsComponent,
    CashFlowComponent,
    ValidateNegativePipe,
    PvWorkingCapitalComponent,
    PvPatrimonyComponent,
    PvOtherProjectionsComponent,
    PvFinancialDebtComponent,
    CapexComponent,
    ValuationComponent,
    NumberTransformPipe,
    PvAssetDepreciationComponent,
    PvDepreciationNewCapexComponent,
    ReportSearchComponent,
    PvParametricModelComponent,
    PvInitValorationComponent,
    NotificationComponent,
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    BrowserAnimationsModule,
    FormsModule,
    ReactiveFormsModule,
    HttpClientModule,
    NgbModule,
    NgxBootstrapMultiselectModule,
    NgxSpinnerModule,
    NgIdleKeepaliveModule.forRoot(),
  ],
  schemas: [CUSTOM_ELEMENTS_SCHEMA],
  providers: [ProfitLossService, { provide: LOCALE_ID, useValue: 'es' }, { provide: HTTP_INTERCEPTORS, useClass: InterceptorService, multi: true },
    { provide: MSAL_INSTANCE, useFactory: MSLAInstanceFactory }, MsalService],
  bootstrap: [AppComponent],
})
export class AppModule { }

registerLocaleData(localeEs, 'es');