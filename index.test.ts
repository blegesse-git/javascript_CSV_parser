import { expect, use } from "chai";
import chaiAsPromised = require('chai-as-promised');
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
  before(async () => {
    use(chaiAsPromised)
  });

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

//   it("trim space delimited by space", async () => { // TODO: test fails 
//     const parser = new CSVParser({
//       delimiter: " ", 
//       headerRowCount: 1, 
//       trimSpace: true,
//     });

//     const testCSV = `   first   second   third   fourth
// abcdefgh        0       2    false
// abcdef      3.3       4     true
// f        0       2    false`;

//     const expectedRecordFields = {
//       "first":  "abcdef",
//       "second": 3.3,
//       "third":  4,
//       "fourth": true,
//     };

//     const metrics = await parser.parse(testCSV);
//     // console.log('metrics:', metrics)
//     // expect(metrics[1]?.recordFields).to.be.eql(expectedRecordFields);
//   })

  it("skips rows", async () => {
    const parser = new CSVParser({
      headerRowCount: 1, 
      skipRows: 1, 
      tagColumns: ["line1"],
      measurementColumn: "line3"
    });

    const testCSV = `garbage nonsense
    line1,line2,line3
    hello,80,test_name2`;

    const expectedRecordFields = {
      "line2": 80
    };

    const expectedTags = {
      "line1": "hello",
    };

    const metrics = await parser.parse(testCSV);
    expect("test_name2").to.be.eql(metrics[0]?.name);
    expect(expectedRecordFields).to.be.eql(metrics[0]?.recordFields);
    expect(expectedTags).to.be.eql(metrics[0]?.tags);

    // test with csv rows 
    const parser2 = new CSVParser({
      headerRowCount: 1, 
      skipRows: 1, 
      tagColumns: ["line1"],
      measurementColumn: "line3"
    });
    const testCSVRows: any = ["garbage nonsense\r\n", "line1,line2,line3\r\n", "hello,80,test_name2\r\n"];

    await expect(parser2.parse(testCSVRows[0])).to.be.rejectedWith(Error)
    await expect(() => parser2.parse(testCSVRows[1])).to.not.throw();
    await expect(() => parser2.parse(testCSVRows[2])).to.not.throw();

    const metrics2 = await parser2.parse(testCSVRows[2]);
    expect("test_name2").to.be.eql(metrics2[0]?.name);
    expect(expectedRecordFields).to.be.eql(metrics2[0]?.recordFields);
    expect(expectedTags).to.be.eql(metrics2[0]?.tags);

  })

  it("skips columns", async () => {
    const parser = new CSVParser({
      skipColumns: 1, 
      columnNames: ["line1", "line2"], 
    });

    const testCSV = `hello,80,test_name`;

    const expectedRecordFields = {
      "line1": 80, 
      "line2": "test_name"
    };

    const metrics = await parser.parse(testCSV);
    expect(expectedRecordFields).to.be.eql(metrics[0]?.recordFields);

  })

  it("skips columns with header", async () => {
    const parser = new CSVParser({
      skipColumns: 1, 
      headerRowCount: 2,
    });

    const testCSV = `col,col,col
1,2,3
trash,80,test_name`;

    const expectedRecordFields = {
      "col2": 80, 
      "col3": "test_name"
    };

    const metrics = await parser.parse(testCSV);
    expect(expectedRecordFields).to.be.eql(metrics[0]?.recordFields);

  })

  it("can parse with multi header config", async () => {
    const parser = new CSVParser({
      headerRowCount: 2,
    });

    const testCSV = `col,col
1,2
80,test_name`;

    const expectedRecordFields = {
      "col1": 80, 
      "col2": "test_name"
    };

    const metrics = await parser.parse(testCSV);
    expect(expectedRecordFields).to.be.eql(metrics[0]?.recordFields);

    const testCSVRows: any = ["col,col\r\n", "1,2\r\n", "80,test_name\r\n"];

    const parser2 = new CSVParser({
      headerRowCount: 2,
    });

    await expect(parser2.parse(testCSVRows[0])).to.be.rejectedWith(Error)
    await expect(() => parser2.parse(testCSVRows[1])).to.not.throw();

    const metrics2 = await parser2.parse(testCSVRows[2]);
    expect(expectedRecordFields).to.be.eql(metrics2[0]?.recordFields);


  })

  it("parses stream", async () => {
    const parser = new CSVParser({
      metricName: "csv", 
      headerRowCount: 1,
    });

    const csvHeader = "a,b,c";
    const csvBody = "1,2,3";

    const metrics = await parser.parse(csvHeader);
    expect(metrics.length).to.be.eql(0);

    const metrics2: any = await parser.parseLine(csvBody);
    expect(Object.values(metrics2)).deep.equal(["csv", {}, {"a": 1, "b": 2, "c": 3}, new Date()])

  })

  it("throws multi metric error message", async () => {
    const parser = new CSVParser({
      metricName: "csv", 
      headerRowCount: 1,
    });

    const csvHeader = "a,b,c";
    const csvOneRow = "1,2,3";
    const csvTwoRows = "4,5,6\n7,8,9";

    const metrics = await parser.parse(csvHeader);
    expect(metrics.length).to.be.eql(0);

    const metrics2: any = await parser.parseLine(csvOneRow);
    expect(Object.values(metrics2)).deep.equal(["csv", {}, {"a": 1, "b": 2, "c": 3}, new Date()])

    await expect(parser.parseLine(csvTwoRows)).to.be.rejectedWith(Error, 'Expected 1 metric found 2')

    const metrics3 = await parser.parse(csvTwoRows);
    expect(metrics3.length).to.be.eql(2);


  })

  // it("testing time stamp unix float precision", async () => { // test failes
  //   const parser = new CSVParser({
  //     metricName: "csv", 
  //     columnNames: ["time", "value"],
  //     timestampColumn: "time",
  //     timestampFormat: "unix",
  //   });

  //   const data = `1551129661.95456123352050781250,42`;

  //   const metrics = await parser.parse(data);
  //   console.log(metrics);
    


  // })
  // it("skips measurement column", async () => { // test failes
  //   const parser = new CSVParser({
  //     metricName: "csv", 
  //     headerRowCount: 1,
  //     timestampColumn: "timestamp",
  //     timestampFormat: "unix",
  //     trimSpace: true,
  //   });

  //   const data = `id,value,timestamp
	// 	1,5,1551129661.954561233`;

  //   const expected = {
  //     name: "csv",
  //     tags: {},
  //     recordFields: {
  //       "id": 1, 
  //       "value": 5
  //     },
  //     // time: time.Unix(1551129661, 954561233)
  //   }

  //   const metrics = await parser.parse(data);
  //   console.log(metrics);
    


  // })

  // it("time stamp time zone ", async () => { // test failes
  //     const parser = new CSVParser({
  //       headerRowCount: 1,
  //       columnNames: ["first", "second", "third"],
  //       measurementColumn: "third",
  //       timestampColumn: "first",
  //       timestampFormat: "02/01/06 03:04:05 PM",
  //       timezone: "Asia/Jakarta",
  //     });
  
  //     const testCSV = `line1,line2,line3
  //     23/05/09 11:05:06 PM,70,test_name
  //     07/11/09 11:05:06 PM,80,test_name2`;
  
  //     const metrics = await parser.parse(testCSV);
  //     console.log(metrics);
  //   })
  it("can handle empty measurement name", async () => { // time assertion fails
    const parser = new CSVParser({
      metricName: "csv", 
      headerRowCount: 1,
      columnNames: ["", "b"],
      measurementColumn: ""
    });

    const testCSV = `,b
1,2`;

    const metrics = await parser.parse(testCSV);
   
    const expected = {
      name: "csv",
      tags: {},
      recordFields: {
        "b": 2,
      },
      // time: time.unix(0,0)
    }
    expect(expected.name).to.be.eql(metrics[0]?.name);
    expect(expected.tags).to.be.eql(metrics[0]?.tags);
    expect(expected.recordFields).to.be.eql(metrics[0]?.recordFields);
    // expect(expected.time).to.be.eql(metrics[0]?.time);

  });

  it("handles numeric measurement name", async () => { // ignoring time field
    const parser = new CSVParser({
      metricName: "csv", 
      headerRowCount: 1,
      columnNames: ["a", "b"],
      measurementColumn: "a"
    });

    const testCSV = `a,b
1,2`;

    const metrics = await parser.parse(testCSV);
   
    const expected = {
      name: "1",
      tags: {},
      recordFields: {
        "b": 2,
      },
      // time: time.unix(0,0)
    }
    expect(expected.name).to.be.eql(metrics[0]?.name);
    expect(expected.tags).to.be.eql(metrics[0]?.tags);
    expect(expected.recordFields).to.be.eql(metrics[0]?.recordFields);
    // expect(expected.time).to.be.eql(metrics[0]?.time);

  });

  it("can handle static measurement name", async () => { // ignoring time field
    const parser = new CSVParser({
      metricName: "csv", 
      headerRowCount: 1,
      columnNames: ["a", "b"],
    });

    const testCSV = `a,b
1,2`;

    const metrics = await parser.parse(testCSV);
   
    const expected = {
      name: "csv",
      tags: {},
      recordFields: {
        "a": 1,
        "b": 2,
      },
      // time: time.unix(0,0)
    }
    expect(expected.name).to.be.eql(metrics[0]?.name);
    expect(expected.tags).to.be.eql(metrics[0]?.tags);
    expect(expected.recordFields).to.be.eql(metrics[0]?.recordFields);
    // expect(expected.time).to.be.eql(metrics[0]?.time);

  });

  it("skips empty string value", async () => { // ignoring time field
    const parser = new CSVParser({
      metricName: "csv", 
      headerRowCount: 1,
      columnNames: ["a", "b"],
      skipValues: [""]
    });

    const testCSV = `a,b
1,""`;

    const metrics = await parser.parse(testCSV);
   
    const expected = {
      name: "csv",
      tags: {},
      recordFields: {
        "a": 1,
      },
      // time: time.unix(0,0)
    }
    expect(expected.name).to.be.eql(metrics[0]?.name);
    expect(expected.tags).to.be.eql(metrics[0]?.tags);
    expect(expected.recordFields).to.be.eql(metrics[0]?.recordFields);
    // expect(expected.time).to.be.eql(metrics[0]?.time);

  });

  it("skips specified string value", async () => { // ignoring time field
    const parser = new CSVParser({
      metricName: "csv", 
      headerRowCount: 1,
      columnNames: ["a", "b"],
      skipValues: ["MM"]
    });

    const testCSV = `a,b
1,MM`;

    const metrics = await parser.parse(testCSV);
   
    const expected = {
      name: "csv",
      tags: {},
      recordFields: {
        "a": 1,
      },
      // time: time.unix(0,0)
    }
    expect(expected.name).to.be.eql(metrics[0]?.name);
    expect(expected.tags).to.be.eql(metrics[0]?.tags);
    expect(expected.recordFields).to.be.eql(metrics[0]?.recordFields);
    // expect(expected.time).to.be.eql(metrics[0]?.time);

  });

  it("skips error on corrupted CSV line", async () => { // test fails
    const parser = new CSVParser({
      headerRowCount: 1,
      timestampColumn: "date",
      timestampFormat: "02/01/06 03:04:05 PM",
      skipErrors: true,
    });

    const testCSV = `date,a,b
23/05/09 11:05:06 PM,1,2
corrupted_line
07/11/09 04:06:07 PM,3,4`;

    const expectedRecordFields0 = {
      "a": 1,
      "b": 2,
    }

    const expectedRecordFields1 = {
      "a": 3,
      "b": 4,
    }
    const metrics = await parser.parse(testCSV);
  //  console.log(metrics);
    // expect(expectedRecordFields0).to.be.eql(metrics[0]?.recordFields);
    // expect(expectedRecordFields1).to.be.eql(metrics[1]?.recordFields);
  });

  it("can parse with metadata separators", async () => { 
    let parser

    expect(() => { 
      parser = new CSVParser({
        columnNames: ["a", "b"],
        metadataRows: 0, 
        metadataSeparators: []
      })
    }).to.not.throw(Error);
    
    let parser2
   
    expect(() => { 
      parser2 = new CSVParser({
        columnNames: ["a", "b"],
        metadataRows: 1, 
        metadataSeparators: []
      })
    }).to.throw(Error, "metadataSeparators required when specifying metadataRows");
    
    const parser3 = new CSVParser({
      columnNames: ["a", "b"],
      metadataRows: 1, 
      metadataSeparators: [",", "=", ",", ":", "=", ":="]
    });

    expect(parser3.metadataSeparatorList.length).to.equal(4);
    expect(parser3.config.metadataTrimSet.length).to.equal(0);
    expect(parser3.metadataSeparatorList).deep.equal([",", "=", ":", ":="]);

    const parser4 = new CSVParser({
      columnNames: ["a", "b"],
      metadataRows: 1, 
      metadataSeparators: [",", ":", "=", ":="],
      metadataTrimSet: " #'"
    });

    expect(parser4.metadataSeparatorList.length).to.equal(4);
    expect(parser4.config.metadataTrimSet.length).to.equal(3);
    expect(parser4.metadataSeparatorList).deep.equal([",", ":", "=", ":="]);
  });

})
