import { expect } from "chai";
import { CSVParser } from "./parser";

interface Metric {
  name: string;
  tags: any;
  time: any;
  recordFields: any;
}

const metric: Metric[] = [];

class Metric {
  constructor(name: any, tags: any, time: any, recordFields: any) {
    this.name = name;
    this.tags = tags;
    this.time = time;
    this.recordFields = recordFields;
  }
}

describe("CSVParser", () => {
  it("Tests Basic CSV", async () => {
    const parser = new CSVParser({
      columnNames: ["first", "second", "third"],
      tagColumns: ["third"],
    });
    const metric = await parser.parseLine("1.4,true,hi");
    expect(metric).to.not.be.null;
  });

  it("Tests HeaderConcatenation CSV", async () => {
    const parser = new CSVParser({
      headerRowCount: 2,
      measurementColumn: "3",
    });
    const file = `first,second
  1,2,3
  3.4,70,test_name`;
    const metrics = await parser.parse(file);
    expect(metrics[0]?.name).to.be.eql("test_name");
  });

  it("TestExample1", async () => {
    const parser = new CSVParser({
      headerRowCount: 1,
      timestampColumn: "time",
      timestampFormat: "2006-01-02T15:04:05Z07:00",
      measurementColumn: "measurement",
    });
    const file = `measurement,cpu,time_user,time_system,time_idle,time
    cpu,cpu0,42,42,42,2018-09-13T13:03:28Z`;
    const metrics = await parser.parse(file);
    expect(metrics[0]?.name).to.be.eql("cpu");
  });

  it("TestExample2", async () => {
    const parser = new CSVParser({
      metadataRows: 2,
      metadataSeparators: [":", "="],
      metadataTrimSet: " #",
      headerRowCount: 1,
      tagColumns: ["Version", "File Created"],
      timestampColumn: "time",
      timestampFormat: "2006-01-02T15:04:05Z07:00",
    });
    const file = `# Version=1.1
# File Created: 2021-11-17T07:02:45+10:00
measurement,cpu,time_user,time_system,time_idle,time
cpu,cpu0,42,42,42,2018-09-13T13:03:28Z`;
    const metrics = await parser.parse(file);
    expect(Object.keys(metrics[0]?.tags)).deep.equal([
      "Version",
      "File Created",
    ]);
  });

  it("skips comment", async () => {
    const parser = new CSVParser({
      headerRowCount: 0,
      comment: '#',
      columnNames: ["first", "second", "third", "fourth"],
      metricName: "test_value",
    });
    const file = `#3.3,4,true,hello
    4,9.9,true,name_this`;

    const expectedRecordFields = {
      "first": 4,
      "second": 9.9,
      "third": true, 
      "fourth": "name_this"
    }
    const metrics = await parser.parse(file);
    expect(metrics[0]?.recordFields).to.be.eql(expectedRecordFields);
  });

  it("trims space", async () => {
    const parser = new CSVParser({
      headerRowCount: 0,
      trimSpace: true,
      columnNames: ["first", "second", "third", "fourth"],
      metricName: "test_value",
    });
    const file = ` 3.3, 4,    true,hello`;

    const expectedRecordFields = {
      "first": 3.3,
      "second": 4,
      "third": true, 
      "fourth": "hello"
    }
    const metrics = await parser.parse(file);
    expect(metrics[0]?.recordFields).to.be.eql(expectedRecordFields);

    const parser2 = new CSVParser({
      headerRowCount: 2, 
      trimSpace: true,
    });

    const testCSV = "   col  ,  col  ,col\n" +
		"  1  ,  2  ,3\n" +
		"  test  space  ,  80  ,test_name"

    const metrics2 = await parser2.parse(testCSV);
    const expectedRecordFields2 = {
      "col1": "test  space", 
      "col2": 80,
      "col3": "test_name", 
    }
    expect(metrics2[0]?.recordFields).to.be.eql(expectedRecordFields2);

  });

  it("tests header override", async () => {
    const parser = new CSVParser({
      headerRowCount: 1,
		  columnNames: ["first", "second", "third"],
		  measurementColumn: "third",
    })

    const testCSV = `line1,line2,line3
    3.4,70,test_name`

    const expectedRecordFields = {
      "first":  3.4,
		  "second": 70,
    }

    const metrics = await parser.parse(testCSV);
    expect(metrics[0]?.name).to.be.eql('test_name');
    expect(metrics[0]?.recordFields).to.be.eql(expectedRecordFields);

    const testCSVRows:any = ["line1,line2,line3\r\n", "3.4,70,test_name\r\n"];

    const parser2 = new CSVParser({
      headerRowCount: 1,
		  columnNames: ["first", "second", "third"],
		  measurementColumn: "third",
    })

    const metrics2 = await parser2.parse(testCSVRows[0]);
    expect(metrics2).to.be.eql(metric);

    const metrics3 = await parser2.parseLine(testCSVRows[1]);
    expect(metrics3?.name).to.be.eql('test_name');
    expect(metrics3?.recordFields).to.be.eql(expectedRecordFields);


  })

  it("Tests quoted characters", async () => {
    const parser = new CSVParser({
      headerRowCount: 1,
		  columnNames: ["first", "second", "third"],
		  measurementColumn: "third",
    })

    const testCSV = `line1,line2,line3
"3,4",70,test_name`

    const metrics = await parser.parse(testCSV);
    expect(metrics[0]?.recordFields["first"]).to.be.eql("3,4");
  })

  it("Tests delimeters", async () => {
    const parser = new CSVParser({
      headerRowCount: 1, 
      delimiter: "%", 
      columnNames: ["first", "second", "third"], 
      measurementColumn: "third"
    })

    const testCSV = `line1%line2%line3
3,4%70%test_name`

    const metrics = await parser.parse(testCSV);
    expect(metrics[0]?.recordFields["first"]).to.be.eql("3,4");

  })

  it("Tests value conversion", async () => {
    const parser = new CSVParser({
      headerRowCount: 0, 
      delimiter: ",", 
      columnNames: ["first", "second", "third", "fourth"], 
      metricName: "test_value",
    })

    const testCSV = `3.3,4,true,hello`

    const expectedRecordFields = {
      "first": 3.3,
      "second": 4, 
      "third": true,
      "fourth": "hello"
    }
    const metrics = await parser.parse(testCSV);
    const expectedMetric = new Metric("test_value",{}, new Date(), expectedRecordFields);
    const returnedMetric = new Metric(metrics[0]?.name, metrics[0]?.tags, new Date(), metrics[0]?.recordFields)

    expect(expectedMetric).deep.equal(returnedMetric);

    // // Test explicit type conversion.
    const parser2 = new CSVParser({
      headerRowCount: 0, 
      delimiter: ",", 
      columnNames: ["first", "second", "third", "fourth"], 
      metricName: "test_value",
      columnTypes: ["float", "int", "bool", "string"]
    })

    const metrics2 = await parser2.parse(testCSV);
    const returnedMetric2 = new Metric(metrics2[0]?.name, metrics2[0]?.tags, new Date(), metrics2[0]?.recordFields)
    expect(expectedMetric.recordFields).deep.equal(returnedMetric2.recordFields);

  })

  it("skips comments", async () => {
    const parser = new CSVParser({
      headerRowCount: 0, 
      comment: "#", 
      columnNames: ["first", "second", "third", "fourth"],
      metricName: "test_value",
    });

    const testCSV = `#3.3,4,true,hello
    4,9.9,true,name_this`;

    const expectedRecordFields = {
      "first": 4, 
      "second": 9.9, 
      "third": true,
      "fourth": "name_this"
    }

    const metrics = await parser.parse(testCSV);
    expect(metrics[0]?.recordFields).to.be.eql(expectedRecordFields);
  })

  it("trims space", async () => {
    const parser = new CSVParser({
      headerRowCount: 0, 
      trimSpace: true,
      columnNames: ["first", "second", "third", "fourth"],
      metricName: "test_value",
    })

    const testCSV = ` 3.3, 4,    true,hello`;

    const expectedRecordFields = {
      "first": 3.3, 
      "second": 4,
      "third": true,
      "fourth": "hello"
    };

    const metrics = await parser.parse(testCSV);
    expect(metrics[0]?.recordFields).to.be.eql(expectedRecordFields);

    const parser2 = new CSVParser({
      headerRowCount: 2, 
      trimSpace: true,
    })

    const testCSV2 = "   col  ,  col  ,col\n" +
		"  1  ,  2  ,3\n" +
		"  test  space  ,  80  ,test_name";

    const metrics2 = await parser2.parse(testCSV2);

    const expectedRecordFields2 = {
      "col1": "test  space", 
      "col2": 80,
      "col3": "test_name",
    };
    expect(metrics2[0]?.recordFields).to.be.eql(expectedRecordFields2);
  })
})
