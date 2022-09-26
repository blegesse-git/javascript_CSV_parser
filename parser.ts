import { EOFError, LocalFile } from "./utils";
import { CSVReader } from "./reader";

interface Metric {
  name: string;
  tags: any;
  time: any;
  recordFields: any;
}

interface ParserConfig {
  columnNames: string[];
  columnTypes: string[];
  comment: string;
  defaultTags: Record<string, string>;
  delimiter: string;
  headerRowCount: number;
  measurementColumn: string;
  metricName: string;
  skipColumns: number;
  skipRows: number;
  tagColumns: string[];
  timestampColumn: string;
  timestampFormat: string;
  timezone: string;
  trimSpace: boolean;
  skipValues: string[];
  skipErrors: boolean;
  metadataRows: number;
  metadataSeparators: string[];
  metadataTrimSet: string;
  resetMode: string;
}

type TimeFunc = () => Date;

export class CSVParser implements ParserConfig {
  columnNames: string[];
  columnTypes: string[];
  comment: string;
  defaultTags: Record<string, string>;
  delimiter: string;
  headerRowCount: number;
  measurementColumn: string;
  metricName: string;
  skipColumns: number;
  skipRows: number;
  tagColumns: string[];
  timestampColumn: string;
  timestampFormat: string;
  timezone: string;
  trimSpace: boolean;
  skipValues: string[];
  skipErrors: boolean;
  metadataRows: number;
  metadataSeparators: string[];
  metadataTrimSet: string;
  resetMode: string;

  private timeFunc: TimeFunc = () => new Date();
  private gotColumnNames = false;
  private gotInitialColumnNames = false;
  private remainingSkipRows = 0;
  private remainingHeaderRows = 0;
  private remainingMetadataRows = 0;
  private metadataTags: Record<string, string> = {};
  private metadataSeparatorList: string[] = [];

  // InitFromConfig
  // Partial type sets all properties to optional
  constructor(config?: Partial<ParserConfig>) {
    config = config ?? {};
    this.columnNames = config.columnNames ?? [];
    this.columnTypes = config.columnTypes ?? [];
    this.comment = config.comment ?? "";
    this.defaultTags = config.defaultTags ?? {};
    this.delimiter = config.delimiter ?? "";
    this.headerRowCount = config.headerRowCount ?? 0;
    this.measurementColumn = config.measurementColumn ?? "";
    this.metricName = config.metricName ?? ""; // part of parser type in go parser
    this.skipColumns = config.skipColumns ?? 0;
    this.skipRows = config.skipRows ?? 0;
    this.tagColumns = config.tagColumns ?? [];
    this.timestampColumn = config.timestampColumn ?? "";
    this.timestampFormat = config.timestampFormat ?? "";
    this.timezone = config.timestampFormat ?? "";
    this.trimSpace = config.trimSpace ?? true;
    this.skipValues = config.skipValues ?? []; // part of parser type in go parser
    this.skipErrors = config.skipErrors ?? true; // part of parser type in go parser
    this.metadataRows = config.metadataRows ?? 0;
    this.metadataSeparators = config.metadataSeparators ?? [];
    this.metadataTrimSet = config.metadataTrimSet ?? "";
    this.resetMode = config.resetMode ?? "none";

    this.init();
  }

  init() {
    if (!this.headerRowCount && !this.columnNames.length) {
      throw new Error(
        "`csv_header_row_count` must be defined if `csv_column_names` is not specified"
      );
    }

    if (this.delimiter.length > 1) {
      throw new Error(
        `csv_delimiter must be a single character, got: ${this.delimiter}`
      );
    }

    if (this.comment.length > 1) {
      throw new Error(
        `csv_comment must be a single character, got: ${this.comment}`
      );
    }

    this.gotInitialColumnNames = !!this.columnNames.length;
    if (
      this.columnNames.length &&
      this.columnTypes.length &&
      this.columnNames.length === this.columnTypes.length
    ) {
      throw new Error(
        "csv_column_names field count doesn't match with csv_column_types"
      );
    }

    this.initializeMetadataSeparator();

    if (!this.resetMode) {
      this.resetMode = "none";
    }

    if (!["none", "always"].includes(this.resetMode)) {
      throw new Error(`unknown reset mode ${this.resetMode}`);
    }

    this.reset();
  }

  async parse(file: LocalFile) {
    // Reset the parser according to the specified mode
    if (this.resetMode == "always") {
      this.reset();
    }

    return this.parseCSV(file);
  }

  async parseLine(line: LocalFile) {
    if (!line) {
      if (this.remainingSkipRows > 0) {
        this.remainingSkipRows--;
        throw EOFError;
      }
      if (this.remainingMetadataRows > 0) {
        this.remainingMetadataRows--;
        throw EOFError;
      }
    }

    const metrics = await this.parseCSV(line);
    if (metrics.length === 1) {
      return metrics[0]!;
    }
    if (metrics.length > 1) {
      throw new Error(`Expected 1 metric found ${metrics.length}`);
    }
    return null;
  }

  reset() {
    // Reset the columns if they were not user-specified
    this.gotColumnNames = this.gotInitialColumnNames;
    if (!this.gotInitialColumnNames) {
      this.columnNames = [];
    }

    // Reset the internal counters
    this.remainingHeaderRows = this.headerRowCount;
    this.remainingMetadataRows = this.metadataRows;
    this.remainingSkipRows = this.skipRows;
  }

  setDefaultTags(tags: Record<string, string>) {
    this.defaultTags = tags;
  }

  setTimeFunc(fn: TimeFunc) {
    this.timeFunc = fn;
  }

  private compile(file: LocalFile) {
    return new CSVReader({
      file,
      delimiter: this.delimiter,
      comment: this.comment,
      trimSpace: this.trimSpace,
    });
  }

  private initializeMetadataSeparator() {
    if (this.metadataRows <= 0) return;

    if (!this.metadataSeparators.length) {
      throw new Error(
        "csv_metadata_separators required when specifying csv_metadata_rows"
      );
    }

    const patternList: Record<string, boolean> = {};
    for (const pattern of this.metadataSeparators) {
      if (patternList[pattern]) {
        // Ignore further, duplicated entries
        continue;
      }
      patternList[pattern] = true;
      this.metadataSeparatorList.push(pattern);
    }

    this.metadataSeparatorList.sort((a, b) => a.length - b.length);
  }

  private async parseCSV(file: LocalFile) {
    file = typeof file != "string" ? await file.text() : file;

    // Skip first rows
    while (this.remainingSkipRows > 0) {
      let { text } = this.readLine(file);
      file = text;
      this.remainingSkipRows--;
    }

    // Parse metadata
    while (this.remainingMetadataRows > 0) {
      let { line, text } = this.readLine(file);
      file = text;
      this.remainingMetadataRows--;
      const m = this.parseMetadataRow(line);
      for (const k in m) {
        this.metadataTags[k] = m[k]!;
      }
    }

    const csvReader = this.compile(file);
    // If there is a header, and we did not get DataColumns
    // set DataColumns to names extracted from the header
    // we always reread the header to avoid side effects
    // in cases where multiple files with different
    // headers are read
    while (this.remainingHeaderRows > 0) {
      const headers: string[] = csvReader.read();
      this.remainingHeaderRows--;
      if (this.gotColumnNames) {
        // Ignore header lines if columns are named
        continue;
      }

      // Concatenate header names
      for (let [i, header] of headers.entries()) {
        const name = this.trimSpace ? this.trim(header, " ") : header;
        if (this.columnNames.length <= i) {
          this.columnNames.push(name);
        } else {
          this.columnNames[i] = this.columnNames[i] + name;
        }
      }
    }

    if (!this.gotColumnNames) {
      // Skip first rows
      this.columnNames = this.columnNames.slice(this.skipColumns);
      this.gotColumnNames = true;
    }

    const records: string[][] = csvReader.readAll();
    const metrics: Metric[] = [];
    for (const record of records) {
      try {
        const metric = this.parseRecord(record);
        metrics.push(metric);
      } catch (err) {
        if (this.skipErrors) {
          console.error(err);
          continue;
        }
        throw err;
      }
    }
    return metrics;
  }

  private parseMetadataRow(haystack: string) {
    haystack = this.trimRight(haystack, "\r\n");
    for (const needle of this.metadataSeparatorList) {
      const metadata = haystack.split(needle, 2);
      if (metadata.length < 2) {
        continue;
      }
      const key = this.trim(metadata[0]!, this.metadataTrimSet);
      if (key) {
        const value = this.trim(metadata[1]!, this.metadataTrimSet);
        return { [key]: value };
      }
    }
    return {};
  }

  private parseRecord(record: string[]): Metric {
    const recordFields: Record<string, any> = {};
    const tags: Record<string, string> = {};

    // Skip columns in record
    record = record.slice(this.skipColumns);
    outer: for (const [i, fieldName] of this.columnNames.entries()) {
      if (i < record.length) {
        const value = this.trimSpace ? this.trim(record[i]!, " ") : record[i]!;

        // Don't record fields where the value matches a skip value
        for (const skipValue of this.skipValues) {
          if (value === skipValue) {
            continue outer;
          }
        }

        for (const tagName of this.tagColumns) {
          if (tagName === fieldName) {
            tags[tagName] = value;
            continue outer;
          }
        }

        // If the field name is the timestamp column, then keep field name as is.
        if (fieldName == this.timestampColumn) {
          recordFields[fieldName] = value;
          continue;
        }

        // Try explicit conversion only when column types is defined.
        if (this.columnTypes.length > 0) {
          // Throw error if current column count exceeds defined types.
          if (i >= this.columnTypes.length) {
            throw new Error("Column type: Column count exceeded");
          }

          let val: any;
          switch (this.columnTypes[i]) {
            case "int":
              val = parseInt(val, 10);
              if (isNaN(val)) {
                throw new Error("Column type: Column is not an integer");
              }
              break;
            case "float":
              val = parseFloat(val);
              if (isNaN(val)) {
                throw new Error("Column type: Column is not a float");
              }
              break;
            case "bool":
              val = parseBool(val);
              if (val === undefined) {
                throw new Error("Column type: Column is not a boolean");
              }
              break;
            default:
              val = value;
          }

          recordFields[fieldName] = val;
          continue;
        }

        // Attempt type conversions
        const iValue = parseInt(value, 10);
        const fValue = parseFloat(value);
        const bValue = parseBool(value);
        if (!isNaN(iValue)) {
          recordFields[fieldName] = iValue;
        } else if (!isNaN(fValue)) {
          recordFields[fieldName] = fValue;
        } else if (bValue !== undefined) {
          recordFields[fieldName] = bValue;
        } else {
          recordFields[fieldName] = value;
        }
      }
    }

    // Add metadata tags
    for (const k in this.metadataTags) {
      tags[k] = this.metadataTags[k]!;
    }

    // Add default tags
    for (const k in this.defaultTags) {
      tags[k] = this.defaultTags[k]!;
    }

    // Will default to plugin name
    const measurementValue = recordFields[this.measurementColumn];
    const doesExist =
      this.measurementColumn &&
      measurementValue != undefined &&
      measurementValue != "";
    const measurementName = !doesExist
      ? this.metricName
      : `${measurementValue}`;

    return {
      name: measurementName,
      tags,
      recordFields,
      time: parseTimestamp({
        timeFunc: this.timeFunc,
        recordFields,
        timestampColumn: this.timestampColumn,
        timestampFormat: this.timestampFormat,
        timezone: this.timezone,
      }),
    };
  }

  private readLine(text: string): { text: string; line: string } {
    const lines = text.split("\n");
    if (!lines.length) {
      throw EOFError;
    }

    return { line: lines[0]!, text: lines.splice(1).join("\n") };
  }

  private trim(s: string, cutset: string) {
    s = this.trimLeft(s, cutset);
    return this.trimRight(s, cutset);
  }

  private trimLeft(s: string, cutset: string) {
    return s.replace(new RegExp(`^[${cutset}]+`), "");
  }

  private trimRight(s: string, cutset: string) {
    return s.replace(new RegExp(`[${cutset}]+$`), "");
  }
}

function parseBool(str: string) {
  switch (str) {
    case "1":
    case "t":
    case "T":
    case "true":
    case "TRUE":
    case "True":
      return true;
    case "0":
    case "f":
    case "F":
    case "false":
    case "FALSE":
    case "False":
      return false;
    default:
      return undefined;
  }
}

interface ParseTimestampOptions {
  timeFunc: TimeFunc;
  recordFields: Record<string, any>;
  timestampColumn: string;
  timestampFormat: string;
  timezone: string;
}

function parseTimestamp({
  timeFunc,
  recordFields,
  timestampColumn,
  timestampFormat,
}: ParseTimestampOptions) {
  if (timestampColumn) {
    if (recordFields[timestampColumn] === undefined) {
      throw new Error(
        `Timestamp column: ${timestampColumn} could not be found`
      );
    }

    switch (timestampFormat) {
      case "":
        throw new Error("Timestamp format must be specified");
      default:
        return new Date(recordFields[timestampColumn]);
    }
  }

  return timeFunc();
}
