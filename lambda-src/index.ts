import { DynamoDB, S3 } from "aws-sdk";
// import * as fs from "fs";
import * as csv from "csv"
// import * as path from "path";
// import { v4 as uuidv4 } from 'uuid';
import * as stream from "stream";

export const handler = async (event, context) => {
  // Lambda code
  await main();
}

const main = async () => {
  //const inputS3FileName = "pob-small.csv"
  //const outputS3FileName = "output-pob-small.txt"
  const inputS3FileName = process.env.INPUT
  const outputS3FileName = process.env.OUTPUT
  // const inputS3FileName = "data3.csv"
  // const outputS3FileName = "output-pob-big.txt"
  const inputBucketName = "test-csv-to-json-input"
  const outputBukcteName = "owner-airflow-etl-data-dev"
  const s3 = new S3()
  console.info(`Reading and Parsing file ${inputS3FileName}...`)
  const source = s3.getObject({ Bucket: inputBucketName, Key: inputS3FileName }).createReadStream()
  const { writeStream, promise } = uploadStream(s3, outputBukcteName, outputS3FileName);
  source.pipe(csv.parse({ columns: true, skipEmptyLines: true }))
    .pipe(
      csv.transform((record) => {
        return converterToDynamodbFormat(record)
      })
    )
    .pipe(writeStream)

  source.on('error', (error) => {
    console.error(`ReadSteam error - ${error.message}`)
  })
  let output = true;
  await promise.catch((reason) => { output = false; console.error(reason); });
  if (output) {
    console.info(`Successfully converted ${inputS3FileName} and saved output file ${outputS3FileName} in bucket ${outputBukcteName}.`)
  }
}

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
  // convertedData.id = { S: uuidv4() }
  if (!convertedData.id) {
    const today = new Date();
    const after7days = new Date(today.getFullYear(), today.getMonth(), today.getDate() + 7);
    const now = after7days.getTime()
    convertedData["expired_at"] = { N: `${now}` }
  }
  
  if (convertedData.id && !convertedData["unit_id"]["S"]) {
    convertedData["unit_id"] = { S: "null" }
  }
  const JsonData = `${JSON.stringify(convertedData)}\n`;
  return replaceObjectName(JsonData);
};

const uploadStream = (S3: AWS.S3, Bucket: string, Key: string) => {
  const passT = new stream.PassThrough();
  return {
    writeStream: passT,
    promise: S3.upload({ Bucket, Key, Body: passT }).promise(),
  };
};


// try {
//   console.log(`Reading and Parsing file ...`)
//   const outputPath = path.resolve(__dirname, "../output/unit_pob_uuid_full.txt")
//   const source = fs.createReadStream(path.resolve(__dirname, "../data/data3.csv"))
//   const output = fs.createWriteStream(outputPath);
//   source.pipe(csv.parse({ columns: true, skipEmptyLines: true }))
//     .pipe(
//       csv.transform((record) => {
//         return converterToDynamodbFormat(record)
//       })
//     )
//     .pipe(output)

//   source.on('error', (error) => {
//     console.error(`ReadSteam error - ${error.message}`)
//     source.close()
//   })
//   output.on('error', (error) => {
//     console.error(`WriteSteam error - ${error.message}`)
//     output.close()
//   })
//   output.on('finish', () => {
//     console.log(`File convertion done - saved in ${outputPath}`)
//     output.close()
//   })

// } catch (err) {
//   console.error(err);
// }
