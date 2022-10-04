import Papa from "papaparse";
import { EOFError, LocalFile } from "./utils";

interface ReaderConfig {
  delimiter: string;
  comment: string;
  trimSpace: boolean;
  file: LocalFile;
}

export class CSVReader {
  records: string[][] = [];
  constructor({ delimiter, comment, file }: Partial<ReaderConfig>) {
    const records: string[][] = [];
    Papa.parse<any[]>(file!, {
      delimiter,
      comments: comment,
      skipEmptyLines: true,
      
      // step: callback function to help parse large files by streaming. results are sent 
      // to the step callback function row by row.

      // consider the CSV:
      // 1, foo, bar, baz
      // 2, qux, qui, qoo
      // 3, fop, fip,
      step({ data: csvRow }, _) {
        records.push(csvRow.filter((column) => column !== ""));
      },
    });
    this.records = records;
  }

  read() { // this will be used to get the headers hence the shifting 
    const record = this.records.shift();
    if (!record) {
      throw EOFError;
    }

    return record;
  }

  readAll() {
    const records = this.records;
    this.records = [];
    return records;
  }
}
