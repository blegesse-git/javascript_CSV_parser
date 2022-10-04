import dayjs from "dayjs";
import dayjsCustomParseFormat from "dayjs/plugin/customParseFormat";
import dayjsUTC from "dayjs/plugin/utc";
import dayjsTimezone from "dayjs/plugin/timezone";
dayjs.extend(dayjsCustomParseFormat);
dayjs.extend(dayjsUTC);
dayjs.extend(dayjsTimezone);

import { EOFError, LocalFile } from "./utils";
import { CSVReader } from "./reader";

interface Metric {
  name: string;
  tags: Record<string, any>;
  time: Date;
  fields: Record<string, any>;
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
  timeFunc: TimeFunc;
}

type TimeFunc = () => Date;

export class Config {
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
  timeFunc: TimeFunc;

  constructor (
    columnNames: string[],
    columnTypes: string[],
    comment: string,
    defaultTags: Record<string, string>,
    delimiter: string,
    headerRowCount: number,
    measurementColumn: string,
    metricName: string,
    skipColumns: number,
    skipRows: number,
    tagColumns: string[],
    timestampColumn: string,
    timestampFormat: string,
    timezone: string,
    trimSpace: boolean,
    skipValues: string[],
    skipErrors: boolean,
    metadataRows: number,
    metadataSeparators: string[],
    metadataTrimSet: string,
    resetMode: 'none' | 'always',
    timeFunc: TimeFunc
  ) {
    this.columnNames = columnNames ?? [];
    this.columnTypes = columnTypes;
    this.comment = comment;
    this.defaultTags = defaultTags;
    this.delimiter = delimiter;
    this.headerRowCount = headerRowCount;
    this.measurementColumn = measurementColumn;
    this.metricName = metricName;
    this.skipColumns = skipColumns;
    this.skipRows = skipRows;
    this.tagColumns = tagColumns;
    this.timestampColumn = timestampColumn;
    this.timestampFormat = timestampFormat;
    this.timezone = timezone;
    this.trimSpace = trimSpace;
    this.skipValues = skipValues;
    this.skipErrors = skipErrors;
    this.metadataRows = metadataRows;
    this.metadataSeparators = metadataSeparators;
    this.metadataTrimSet = metadataTrimSet;
    this.resetMode = resetMode || 'none';
    this.timeFunc = timeFunc;

    if (!this.headerRowCount && !this.columnNames.length) {
      throw new Error(
        "`headerRowCount` cannot be 0 if `columnNames` is not specified"
      );
    }

    if (this.delimiter.length > 1) {
      throw new Error(
        `delimiter must be a single character, got: ${this.delimiter}`
      );
    }

    // we might consider removing this configuration option and forcing people to use # as comment
    // let's see how it goes in the wild
    if (this.comment.length > 1) {
      throw new Error(
        `comment must be a single character, got: ${this.comment}`
      );
    }

    if (
      this.columnNames.length &&
      this.columnTypes.length &&
      this.columnNames.length !== this.columnTypes.length
    ) {
      throw new Error(
        "columnNames field count doesn't match with columnTypes"
      );
    }

    if (!["none", "always"].includes(this.resetMode)) {
      throw new Error(`expected "none" or "always" but got unknown reset mode ${this.resetMode}`);
    }
  }
}

export class CSVParser {

  private timeFunc: TimeFunc = () => new Date();
  private gotColumnNames = false;
  private gotInitialColumnNames = false;
  private remainingSkipRows = 0;
  private remainingHeaderRows = 0;
  private remainingMetadataRows = 0;
  public metadataTags: Record<string, string> = {};
  public metadataSeparatorList: string[] = [];
  config: Config;

  // InitFromConfig
  // Move these fallback configuration values into the Config constructor then REMOVE THIS COMMENT
  constructor(config?: Partial<ParserConfig>) {
    this.config = new Config(
      config?.columnNames,
      config?.columnTypes ?? [],
      config?.comment ?? "",
      config?.defaultTags ?? {},
      config?.delimiter ?? "",
      config?.headerRowCount ?? 0,
      config?.measurementColumn ?? "",
      config?.metricName ?? "",
      config?.skipColumns ?? 0,
      config?.skipRows ?? 0,
      config?.tagColumns ?? [],
      config?.timestampColumn ?? "",
      config?.timestampFormat ?? "",
      config?.timezone ?? "",
      config?.trimSpace ?? false,
      config?.skipValues ?? [],
      config?.skipErrors ?? true,
      config?.metadataRows ?? 0,
      config?.metadataSeparators ?? [],
      config?.metadataTrimSet ?? "",
      config?.resetMode,
      config?.timeFunc ?? (() => new Date())
    )

    this.gotInitialColumnNames = !!this.config.columnNames.length;

    this.initializeMetadataSeparator();
    this.reset();
  }

  async parse(file: LocalFile) {
    // Reset the parser according to the specified mode
    if (this.config.resetMode === "always") {
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
    if (!this.gotColumnNames) {
      this.config.columnNames = [];
    }

    // Reset the internal counters
    this.remainingHeaderRows = this.config.headerRowCount;
    this.remainingMetadataRows = this.config.metadataRows;
    this.remainingSkipRows = this.config.skipRows;
  }

  setDefaultTags(tags: Record<string, string>) {
    this.config.defaultTags = tags;
  }

  setTimeFunc(fn: TimeFunc) {
    this.timeFunc = fn;
  }

  private compile(file: LocalFile) {
    return new CSVReader({
      file,
      delimiter: this.config.delimiter,
      comment: this.config.comment,
      trimSpace: this.config.trimSpace,
    });
  }

  private initializeMetadataSeparator() {
    if (this.config.metadataRows <= 0) return;

    if (this.config.metadataSeparators.length === 0) {
      throw new Error(
        "metadataSeparators required when specifying metadataRows"
      );
    }

    const patternList: Record<string, boolean> = {};
    for (const pattern of this.config.metadataSeparators) {
      if (patternList[pattern]) {
        // Ignore further, duplicated entries
        continue;
      }
      patternList[pattern] = true;
      this.metadataSeparatorList.push(pattern);
    }

    this.metadataSeparatorList.sort((a, b) => b.length - a.length);
  }

  private async parseCSV(file: LocalFile) {
    let localFile = typeof file === "string" ? file : await file.text();

    // Skip first rows
    while (this.remainingSkipRows > 0) {
      let { text } = this.readLine(localFile);
      localFile = text;
      this.remainingSkipRows--;
    }

    // Parse metadata
    while (this.remainingMetadataRows > 0) {
      let { line, text } = this.readLine(localFile);
      localFile = text;
      this.remainingMetadataRows--;

      const metadata = this.parseMetadataRow(line);
      for (const key in metadata) {
        this.metadataTags[key] = metadata[key]!;
      }
    }

    // CODE REVIEW STOPPED HERE, CONTINUE FROM THIS POINT
    const csvReader = this.compile(localFile);
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
        const name = this.config.trimSpace ? this.trim(header, " ") : header;
        if (this.config.columnNames.length <= i) {
          this.config.columnNames.push(name);
        } else {
          this.config.columnNames[i] = this.config.columnNames[i] + name;
        }
      }
    }

    if (!this.gotColumnNames) {
      // Skip first rows
      this.config.columnNames = this.config.columnNames.slice(this.config.skipColumns);
      this.gotColumnNames = true;
    }

    const records: string[][] = csvReader.readAll();
    const metrics: Metric[] = [];
    for (const record of records) {
      try {
        const metric = this.parseRecord(record);
        metrics.push(metric);
      } catch (err) {
        if (this.config.skipErrors) {
          console.error("Parsing error:", err);
          continue;
        }
        throw err;
      }
    }
    return metrics;
  }

  // separate haystack into key:value pairs
  public parseMetadataRow(haystack: string) {
    // remove newline characters from the haystack string
    const trimmedHaystack = this.trimRight(haystack, "\r\n");

    for (const needle of this.metadataSeparatorList) {
      const metadata = trimmedHaystack.split(needle);
      if (metadata.length < 2) {
        continue;
      }
      metadata[1] = metadata.slice(1).join(needle);
      const key = this.trim(metadata[0]!, this.config.metadataTrimSet);
      if (key) {
        const value = this.trim(metadata[1]!, this.config.metadataTrimSet);
        return { [key]: value };
      }
    }
    return {};
  }

  private parseRecord(record: string[]): Metric {
    const recordFields: Record<string, any> = {};
    const tags: Record<string, string> = {};

    // Skip columns in record
    record = record.slice(this.config.skipColumns);
    outer: for (const [i, fieldName] of this.config.columnNames.entries()) {
      if (i < record.length) {
        const value = this.config.trimSpace ? this.trim(record[i]!, " ") : record[i]!;

        // Don't record fields where the value matches a skip value
        for (const skipValue of this.config.skipValues) {
          if (value === skipValue) {
            continue outer;
          }
        }

        for (const tagName of this.config.tagColumns) {
          if (tagName === fieldName) {
            tags[tagName] = value;
            continue outer;
          }
        }

        // If the field name is the timestamp column, then keep field name as is.
        if (fieldName == this.config.timestampColumn) {
          recordFields[fieldName] = value;
          continue;
        }

        // Try explicit conversion only when column types is defined.
        if (this.config.columnTypes.length > 0) {
          // Throw error if current column count exceeds defined types.
          if (i >= this.config.columnTypes.length) {
            throw new Error("Column type: Column count exceeded");
          }

          let val: any;
          switch (this.config.columnTypes[i]) {
            case "int":
              val = Number(value);
              if (isNaN(val)) {
                throw new Error("Column type: Column is not an integer");
              }
              break;
            case "float":
              val = parseFloat(value);
              if (isNaN(val)) {
                throw new Error("Column type: Column is not a float");
              }
              break;
            case "bool":
              val = parseBool(value);
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
        const iValue = Number(value); // Use this to make timestamp parsing to turn to Nan
        const bValue = parseBool(value);

        if (!isNaN(iValue) ) {
          recordFields[fieldName] = iValue;
        }
        else if (bValue !== undefined) {
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
    for (const k in this.config.defaultTags) {
      tags[k] = this.config.defaultTags[k]!;
    }

    // Will default to plugin name
    const measurementValue = recordFields[this.config.measurementColumn];
    const doesExist =
      this.config.measurementColumn &&
      measurementValue != undefined &&
      measurementValue != "";
    const measurementName = doesExist
      ? `${measurementValue}` : this.config.metricName;


      const metricTime = parseTimestamp({
        timeFunc: this.timeFunc,
        recordFields,
        timestampColumn: this.config.timestampColumn,
        timestampFormat: this.config.timestampFormat,
        timezone: this.config.timezone,
      });

      // Exclude `measurementColumn` and `timestampColumn`
      delete recordFields[this.config.measurementColumn];
      delete recordFields[this.config.timestampColumn];

    return {
      name: measurementName,
      tags,
      fields: recordFields,
      time: metricTime
    };
  }

  // read a line of text, then return the read line and the text with that line removed
  private readLine(text: string): { text: string; line: string } {
    if (text === "") {
      throw EOFError;
    }

    const lines = text.split("\n");
    const firstLine = lines.shift()
    return { line: firstLine!, text: lines.join("\n") };
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
  timezone,
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
        return ParseTimestamp(
          recordFields[timestampColumn],
          timestampFormat,
          timezone
        );
    }
  }

  return timeFunc();
}

export function ParseTimestamp(
  timestamp: any,
  format: string,
  timezone: string
) {
  switch (format.toLowerCase()) {
    case "unix":
      return dayjs(timestamp, "X").toDate();
    case "unix_ms":
    case "unix_us":
    case "unix_ns":
      return dayjs(timestamp, "x").toDate();
    case "iso8601":
      return dayjs(timestamp).toDate();
    default:
      if (!timezone) {
        timezone = "UTC";
      }
      if (typeof timestamp !== "string") {
        throw new Error("Unsupported type");
      }
      return dayjs.tz(timestamp, format, timezone).toDate();
  }
}
