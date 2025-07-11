// === Imports ===
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const express = require('express');
const { Client } = require('pg');
const csv = require('csv-parser');
const copyFrom = require('pg-copy-streams').from;
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify');
const pLimit = require('p-limit');

// === Utils ===
const delay = ms => new Promise(res => setTimeout(res, ms));
let FIELD_TYPES_MAP = {};

// === Configuration ===
const ACCESS_TOKEN = '00DfJ000002QrbH!AQEAQAKuWf9reAXtxwNcs7C1kkIGs06BZfHwpalWftoV8RKaT.MAV6DLDIbs5UFxYvrPRubcYvNc6NCI4m6nGW6QdTzlgxBi';
const INSTANCE_URL = 'https://coresolute4-dev-ed.develop.my.salesforce.com';
const API_VERSION = 'v60.0';

const PG_CONFIG = {
  host: 'dpg-d1i3u8fdiees73cf0dug-a.oregon-postgres.render.com',
  port: 5432,
  database: 'sfdatabase_34oi',
  user: 'sfdatabaseuser',
  password: 'D898TUsAal4ksBUs5QoQffxMZ6MY5aAH',
  ssl: { rejectUnauthorized: false },
  keepAlive: true
};

// === Helper Functions ===
function mapSFTypeToPostgres(sfType) {
  const map = {
    string: 'TEXT', picklist: 'TEXT', textarea: 'TEXT', email: 'TEXT', id: 'TEXT',
    phone: 'TEXT', url: 'TEXT', boolean: 'BOOLEAN', int: 'INTEGER',
    double: 'FLOAT', currency: 'FLOAT', percent: 'FLOAT', date: 'DATE', datetime: 'TIMESTAMP'
  };
  return map[sfType.toLowerCase()] || 'TEXT';
}

async function getAllObjectNames() {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/sobjects`;
  const res = await axios.get(url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });

  return res.data.sobjects
    .filter(o =>
      o.queryable &&
      !o.name.endsWith('__Share') &&
      !o.name.endsWith('__Tag') &&
      !o.name.endsWith('__History') &&
      !o.name.endsWith('__Feed') &&
      !o.name.includes('ChangeEvent') &&
      !o.name.toLowerCase().includes('permissionsetgroup') &&
      !o.name.toLowerCase().includes('recordtype') &&
      !o.name.toLowerCase().includes('recordalerttemplatelocalization')
    )
    .map(o => o.name);
}

async function hasRecords(objectName) {
  try {
    const q = encodeURIComponent(`SELECT count() FROM ${objectName}`);
    const url = `${INSTANCE_URL}/services/data/${API_VERSION}/query?q=${q}`;
    const res = await axios.get(url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });
    return res.data.totalSize > 0;
  } catch {
    return false;
  }
}

async function getAllFields(objectName) {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/sobjects/${objectName}/describe`;
  const res = await axios.get(url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });

  FIELD_TYPES_MAP = {};
  const unsupported = ['address', 'location', 'base64', 'json'];

  let fields = res.data.fields
    .filter(f => !unsupported.includes(f.type) && (!f.compoundFieldName || f.name === 'Name'))
    .map(f => {
      FIELD_TYPES_MAP[f.name] = f.type;
      return f.name;
    });

  if (!fields.includes('Id')) fields.unshift('Id');
  if (!fields.includes('Name')) fields.unshift('Name');

  return [...new Set(fields)];
}

async function createBulkQueryJob(soql) {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/jobs/query`;
  const res = await axios.post(url, {
    operation: 'query',
    query: soql,
    contentType: 'CSV'
  }, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}`, 'Content-Type': 'application/json' } });

  return res.data;
}

async function pollJob(jobId, timeout = 5 * 60 * 1000) {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/jobs/query/${jobId}`;
  let state = 'InProgress';
  const start = Date.now();

  while (state === 'InProgress' || state === 'UploadComplete') {
    if (Date.now() - start > timeout) throw new Error(`Polling timeout for job ${jobId}`);
    const res = await axios.get(url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });
    state = res.data.state;
    if (state === 'JobComplete') return;
    if (['Failed', 'Aborted'].includes(state)) throw new Error(`Job ${state}`);
    await delay(4000);
  }
}

async function downloadResults(jobId) {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/jobs/query/${jobId}/results`;
  const res = await axios.get(url, {
    headers: { Authorization: `Bearer ${ACCESS_TOKEN}` },
    responseType: 'stream'
  });

  const tempFilePath = path.join(__dirname, `temp_${jobId}.csv`);
  const writer = fs.createWriteStream(tempFilePath);
  res.data.pipe(writer);
  return new Promise((resolve, reject) => {
    writer.on('finish', () => resolve(tempFilePath));
    writer.on('error', reject);
  });
}

async function cleanCSV(filePath) {
  const outputPath = filePath.replace('.csv', '_clean.csv');
  const parser = fs.createReadStream(filePath).pipe(parse({
    columns: true,
    relax_quotes: true,
    skip_empty_lines: true,
    relax_column_count: true,
    skip_records_with_error: true
  }));

  const writer = fs.createWriteStream(outputPath);
  const stringifier = stringify({ header: true });

  parser.on('data', row => {
    for (let key in row) row[key] = row[key] === '' ? '\\N' : row[key];
    stringifier.write(row);
  });

  parser.on('end', () => stringifier.end());
  stringifier.pipe(writer);

  return new Promise((resolve, reject) => {
    writer.on('finish', () => resolve(outputPath));
    writer.on('error', reject);
    parser.on('error', reject);
  });
}

async function createTable(client, objectName, headers) {
  const cols = headers.map(h => `"${h}" ${mapSFTypeToPostgres(FIELD_TYPES_MAP[h] || 'string')}`);
  if (headers.includes('Id')) cols[headers.indexOf('Id')] += ' PRIMARY KEY';
  await client.query(`CREATE TABLE IF NOT EXISTS "${objectName}" (${cols.join(', ')});`);
}

async function insertCSVToPostgres(filePath, objectName) {
  const client = new Client(PG_CONFIG);
  await client.connect();

  const tempTable = `"${objectName}_temp"`;

  try {
    const headers = await new Promise((resolve, reject) => {
      fs.createReadStream(filePath).pipe(csv()).on('headers', resolve).on('error', reject);
    });

    // 1. Create main table if not exists
    await createTable(client, objectName, headers);

    // 2. Drop and recreate temp table
    await client.query(`DROP TABLE IF EXISTS ${tempTable};`);
    await client.query(`CREATE TEMP TABLE ${tempTable} AS SELECT * FROM "${objectName}" WITH NO DATA;`);

    // 3. COPY into temp table
    const copySQL = `COPY ${tempTable} (${headers.map(h => `"${h}"`).join(', ')}) FROM STDIN WITH CSV HEADER NULL '\\N'`;
    const stream = client.query(copyFrom(copySQL));
    fs.createReadStream(filePath).pipe(stream);
    await new Promise((resolve, reject) => {
      stream.on('finish', resolve);
      stream.on('error', reject);
    });

    // 4. UPSERT into main table from temp table
    const upsertSQL = `
      INSERT INTO "${objectName}" (${headers.map(h => `"${h}"`).join(', ')})
      SELECT ${headers.map(h => `"${h}"`).join(', ')} FROM ${tempTable}
      ON CONFLICT ("Id") DO UPDATE SET
      ${headers
        .filter(h => h !== 'Id')
        .map(h => `"${h}" = EXCLUDED."${h}"`)
        .join(', ')};
    `;
    await client.query(upsertSQL);

    // 5. Return count of rows in temp table (imported/updated)
    const result = await client.query(`SELECT COUNT(*) FROM ${tempTable};`);
    return parseInt(result.rows[0].count, 10);

  } finally {
    await client.end();
  }
}


async function logBackup({ objectName, recordCount, status, error, csvFilePath }) {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/sobjects/Backup_Log__c`;
  const body = {
    Object_Name__c: objectName,
    Record_Count__c: recordCount,
    Status__c: status,
    Backup_Timestamp__c: new Date().toISOString(),
    Error_Message__c: error || null
  };

  try {
    const res = await axios.post(url, body, {
      headers: {
        Authorization: `Bearer ${ACCESS_TOKEN}`,
        'Content-Type': 'application/json'
      }
    });

    const backupLogId = res.data.id;

    if (status === 'Success' && csvFilePath) {
      const fileData = fs.readFileSync(csvFilePath).toString('base64');
      const fileName = path.basename(csvFilePath);

      const contentVersionRes = await axios.post(
        `${INSTANCE_URL}/services/data/${API_VERSION}/sobjects/ContentVersion`,
        {
          Title: `${objectName}_Backup`,
          PathOnClient: fileName,
          VersionData: fileData
        },
        {
          headers: {
            Authorization: `Bearer ${ACCESS_TOKEN}`,
            'Content-Type': 'application/json'
          }
        }
      );

      const contentVersionId = contentVersionRes.data.id;

      const queryRes = await axios.get(
        `${INSTANCE_URL}/services/data/${API_VERSION}/query?q=` +
        encodeURIComponent(`SELECT ContentDocumentId FROM ContentVersion WHERE Id = '${contentVersionId}'`),
        { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } }
      );

      const contentDocumentId = queryRes.data.records[0].ContentDocumentId;

      await axios.post(
        `${INSTANCE_URL}/services/data/${API_VERSION}/sobjects/ContentDocumentLink`,
        {
          ContentDocumentId: contentDocumentId,
          LinkedEntityId: backupLogId,
          ShareType: 'V'
        },
        {
          headers: {
            Authorization: `Bearer ${ACCESS_TOKEN}`,
            'Content-Type': 'application/json'
          }
        }
      );
    }
  } catch (err) {
    console.error(`❌ Failed to log/attach for ${objectName}: ${err.message}`);
  }
}

async function backupObject(objectName) {
  let rawPath, cleanPath;
  try {
    const hasData = await hasRecords(objectName);
    if (!hasData) {
      console.log(`⏭️ Skipped: ${objectName}`);
      await logBackup({ objectName, recordCount: 0, status: 'Skipped' });
      return;
    }

    const fields = await getAllFields(objectName);
    const soql = `SELECT ${fields.join(', ')} FROM ${objectName}`;
    const job = await createBulkQueryJob(soql);
    await pollJob(job.id);
    rawPath = await downloadResults(job.id);
    cleanPath = await cleanCSV(rawPath);

    const recordCount = await insertCSVToPostgres(cleanPath, objectName);

    // 🚨 Pass CSV path for log attachment
    await logBackup({ objectName, recordCount, status: 'Success', csvFilePath: cleanPath });

    console.log(`✅ Success: ${objectName}`);
  } catch (err) {
    console.warn(`❌ Failed: ${objectName}: ${err.message}`);
    await logBackup({ objectName, recordCount: 0, status: 'Failed', error: err.message });
  } finally {
    // 🔁 Delete files in finally block after all steps
    try {
      if (rawPath && fs.existsSync(rawPath)) fs.unlinkSync(rawPath);
      if (cleanPath && fs.existsSync(cleanPath)) fs.unlinkSync(cleanPath);
    } catch (err) {
      console.warn(`⚠️ Failed to delete temp files: ${err.message}`);
    }
  }
}

// === EXPRESS SERVER ===
const app = express();
app.use(express.json());

app.get('/', (_, res) => res.send('✅ Salesforce Backup Service Running.'));

// ✅ FIXED: Respond immediately, run backup in background
app.post('/api/backup', async (req, res) => {
  try {
    const inputList = req.body.objectNames || [];
    const allObjects = await getAllObjectNames();
    const selected = inputList.length
      ? allObjects.filter(o => inputList.includes(o))
      : allObjects;

    res.json({ message: '✅ Backup started in background.', objects: selected });

    // Background backup
    process.nextTick(async () => {
      const limit = pLimit(1);
      for (let i = 0; i < selected.length; i++) {
        await limit(() => backupObject(selected[i]));
      }
    });

  } catch (err) {
    console.error('❌ API Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
if (require.main === module) {
  app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
}
