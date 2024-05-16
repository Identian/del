import * as XLSX from "xlsx";

export class TableUtil {

    exportTableToExcel(tableId: string, name?: string) {
        let { sheetName, fileName } = getFileName(name!);
        let targetTableElm = document.getElementById(tableId);
        let wb = XLSX.utils.table_to_book(targetTableElm, <XLSX.Table2SheetOpts>{
            sheet: sheetName,
            raw: true
        });
        XLSX.writeFile(wb, `${fileName}.xlsx`);
    }
}

const getFileName = (name: string) => {
    let timeSpan = new Date().toLocaleString('es-CO')
    let sheetName = name || "ExportResult";
    let fileName = `${sheetName}-${timeSpan}`;
    return {
        sheetName,
        fileName
    };
};

export function convertDate(date: string) {

    let dateParts: any[] = date.split("-");
    let unionDate = [dateParts[2], dateParts[1], dateParts[0]].join('-')
    return new Date(unionDate)

}