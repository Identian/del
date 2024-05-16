export interface IProjection {
    name: string,
    projections: number[],
    year: number
    capitulo:string
}

export interface IStatic {
    account: string,
    average: string,
    median: string,
    deviation: string,
    min: string,
    max: string,
    beta: string
}

export interface IFileDepurate {
    row: number,
    accounts_column: number,
    filename: string,
    nit: string,
    replace: number,
    selected_columns: Array<any>
}