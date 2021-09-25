import { DynamoDB } from "aws-sdk";
import * as fs from "fs";
import * as csv from "csv"
import path = require("path");
// import { v4 as uuidv4 } from 'uuid';
const replaceObjectName = (data: string | undefined): string => {
  if (data) {
    data = data.replace(/"M"/g, '"m"');
    data = data.replace(/"L"/g, '"l"');
    data = data.replace(/"S"/g, '"s"');
    data = data.replace(/"N"/g, '"n"');
  }
  return data;
};

const converterToDynamodbFormat = (data: string): string => {
  const convertedData = DynamoDB.Converter.input(data)["M"];
  // if(!convertedData.id) {
  //   convertedData.id = { S: uuidv4() }
  // }
  const JsonData = `${JSON.stringify(convertedData)}\n`;
  return replaceObjectName(JsonData);
};

try {
  console.log("Reading and Parsing file...")
  const outputPath = path.resolve(__dirname, "../output/booking_com_guest_reviews.txt")
  const source = fs.createReadStream(path.resolve(__dirname, "../data/booking_com_guest_reviews.csv"))
  const output = fs.createWriteStream(outputPath);
  source.pipe(csv.parse({ columns: true, skipEmptyLines: true }))
    .pipe(
      csv.transform((record) => {
        return converterToDynamodbFormat(record)
      })
    )
    .pipe(output)

  source.on('error', (error) => {
    console.error(`ReadSteam error - ${error.message}`)
    source.close()
  })
  output.on('error', (error) => {
    console.error(`WriteSteam error - ${error.message}`)
    output.close()
  })
  output.on('finish', () => {
    console.log(`File convertion done - saved in ${outputPath}`)
    output.close()
  })

} catch (err) {
  console.error(err);
}
