export interface IProjection {
    id_assessment: string
    year: number;
    datesHistory: string[]
    datesProjections: string[]
    projection_data: IProjectionData[];
}

export interface IProjectionData {
    name: string
    account: string;
    method: string;
    accountProjector: string;
    atributes: {
        history: number[],
        projection: number[]
    };
    explication: string;
}

