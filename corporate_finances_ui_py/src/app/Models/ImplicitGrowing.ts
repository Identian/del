export interface ImplicitGrowing {
    variations: Values[];
    proportions: Values[];
    statistics: Statistic[];
}

export interface Values {
    name: string;
    value: string[];
}

export interface Statistic {
    account: string;
    average: string;
    median: string;
    min: string;
    max: string;
    deviation: string;
    beta: string;
}
