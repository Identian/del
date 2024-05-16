import { Component, Input } from '@angular/core';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { ProfitLossService } from 'src/app/Common/Services/Assessment/profit-loss.service';
import { ImplicitGrowing } from 'src/app/Models/ImplicitGrowing';
import { DataSubs, IProfitAndLoss, ProfitAndLossData } from 'src/app/Models/Profit-Loss';

@Component({
  selector: 'app-navs-implicit-growth',
  templateUrl: './navs-implicit-growth.component.html'
})
export class NavsImplicitGrowthComponent {

  @Input() profitAndLossData!: IProfitAndLoss
  @Input() accounts: string[] = [];
  @Input() accountsComplete: string[] = [];
  @Input() showList = true
  @Input() implicitGrowthResult: ImplicitGrowing | undefined
  implicitGrowthForm: FormGroup;
  dataAccountPyg: ProfitAndLossData | DataSubs | undefined
  dataAccountProportion: ProfitAndLossData | DataSubs | undefined


  constructor(private formBuilder: FormBuilder, private profitAndLossService: ProfitLossService) {

    this.implicitGrowthForm = this.formBuilder.group({
      accountPyg: new FormControl(null, Validators.required),
      accountProportion: new FormControl(null, Validators.required)
    })

  }

  search() {

    let indexPyg: number = 0;
    let indexProportion: number = 0;

    if (this.isSub(this.implicitGrowthForm.get('accountPyg')?.value)) {

      indexPyg = this.getSubs()?.findIndex(value => value.name == this.implicitGrowthForm.get('accountPyg')?.value)
      indexProportion = this.profitAndLossData.data.findIndex(value => value.name == this.implicitGrowthForm.get('accountProportion')?.value) as number
      this.dataAccountPyg = this.getSubs()[indexPyg];
      this.dataAccountProportion = this.profitAndLossData.data[indexProportion];

    }
    else if (this.isSub(this.implicitGrowthForm.get('accountProportion')?.value)) {

      indexPyg = this.profitAndLossData.data.findIndex(value => value.name == this.implicitGrowthForm.get('accountPyg')?.value) as number
      indexProportion = this.getSubs()?.findIndex(value => value.name == this.implicitGrowthForm.get('accountProportion')?.value)
      this.dataAccountPyg = this.profitAndLossData.data[indexPyg];
      this.dataAccountProportion = this.getSubs()[indexProportion];

    }
    else {

      indexPyg = this.profitAndLossData.data.findIndex(value => value.name == this.implicitGrowthForm.get('accountPyg')?.value) as number
      indexProportion = this.profitAndLossData.data.findIndex(value => value.name == this.implicitGrowthForm.get('accountProportion')?.value) as number
      this.dataAccountPyg = this.profitAndLossData.data[indexPyg];
      this.dataAccountProportion = this.profitAndLossData.data[indexProportion];

    }

    this.calculate()

  }

  isSub(str: string) {
    return /\d/.test(str);
  }

  getSubs() {

    let subs: DataSubs[] = []

    for (let row of this.profitAndLossData.data) {

      if (row.subs) {

        row.subs.map(sub => {
          subs.push(sub);
        })
      }

    }

    return subs

  }

  calculate() {

    let obj: any = {

      accountPyg: [],
      accountProportion: []

    }

    for (let index = 0; index < this.profitAndLossData.datesHistory.length; index++) {

      obj.accountPyg.push({
        account: this.implicitGrowthForm.get('accountPyg')?.value,
        date: this.profitAndLossData.datesHistory[index],
        value: this.getValues(index, this.implicitGrowthForm.get('accountPyg')?.value)
      })

      obj.accountProportion.push({
        account: this.implicitGrowthForm.get('accountProportion')?.value,
        date: this.profitAndLossData.datesHistory[index],
        value: this.getValues(index, this.implicitGrowthForm.get('accountProportion')?.value)
      })

    }

    this.profitAndLossService.calculateImplicitGrowth(obj).then(response => {
      this.implicitGrowthResult = response
    })

  }


  getValues(indexOfYear: number, account: string) {

    if (this.isSub(account)) {

      let subs = this.getSubs()

      for (let row of subs) {

        if (row.name == account) {
          return row.values.history[indexOfYear].value
        }
        else {
          continue
        }
      }

      return 0


    }
    else {

      for (let index = 0; index < this.profitAndLossData.data.length; index++) {

        if (this.profitAndLossData?.data[index].name == account) {
          return this.profitAndLossData?.data[index].values.history[indexOfYear].value
        }
        else {
          continue
        }
      }

      return 0

    }

  }
}

