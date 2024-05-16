export interface Companie {
    company: Company;
    data: Datum[];
    model: string
}

export interface Company {
    name: string;
    sector: string;
}

export interface Datum {
    date: string;
    periodicity: string;
    id_assessment: number
    user: string
}
