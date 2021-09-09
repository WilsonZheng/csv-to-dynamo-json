import { DynamoDB } from "aws-sdk";
import * as fs from "fs";
import * as csvParse from "csv-parse/lib/sync"
import path = require("path");
const replaceObjectName = (data: string): string => {
  data = data.replace(/"M"/g, '"m"');
  data = data.replace(/"L"/g, '"l"');
  data = data.replace(/"S"/g, '"s"');
  data = data.replace(/"N"/g, '"n"');
  return data;
};

const converterToDynamodbFormat = (data: string): string => {
  const convertedData = DynamoDB.Converter.input(data)["M"];
  const JsonData = JSON.stringify(convertedData);
  return replaceObjectName(JsonData);
};

try {
  const input = fs.readFileSync(path.resolve(__dirname, "../data/data.csv"));
  const records = csvParse(input, {
    columns: true,
    skip_empty_lines: true
  })
  const outputPath = path.resolve(__dirname, "../output/datacsv.txt")
  const wstream = fs.createWriteStream(outputPath);
  records.map(user => {
    wstream.write(converterToDynamodbFormat(user));
    wstream.write("\n");
  });
  console.log(`File convertion done - saved in ${outputPath}`)
} catch (err) {
  console.error(err);
}
