import { expect } from "chai";
import { CSVParser } from "./parser";

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
    // console.log('concat_test', JSON.stringify(metrics));
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
    // console.log('test1', JSON.stringify(metrics));
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
    // console.log('metrics: ' + JSON.stringify(metrics))
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
      "second": 9, // test fails when this values is 9.9
      "third": true, 
      "fourth": "name_this"
    }
    const metrics = await parser.parse(file);
    // console.log('skip comment: ' + JSON.stringify(metrics))
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
      "first": 3, // test fails when value is 3.3
      "second": 4,
      "third": true, 
      "fourth": "hello"
    }
    const metrics = await parser.parse(file);
    // console.log('trim space: ' + JSON.stringify(metrics))
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
    // console.log('trim space2: ' + JSON.stringify(metrics2))
    expect(metrics2[0]?.recordFields).to.be.eql(expectedRecordFields2);

  });
});