export interface IProfitAndLoss {
    datesHistory: string[];
    datesProjection: string[];
    data: ProfitAndLossData[];
}

export interface ProfitAndLossData {
    name: string;
    values: {
        history: Data[];
        projection: Data[];
    };
    subs?: DataSubs[];
}

export interface Data {
    hint: string;
    value: number
}

export interface DataSubs {
    name: string;
    values: {
        history: Data[];
        projection?: Data[];
    };
}