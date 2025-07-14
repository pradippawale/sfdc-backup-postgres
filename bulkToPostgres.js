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
const ACCESS_TOKEN = '00DfJ000002QrbH!AQEAQCguWQ87WidUu3Hg2ai3eckdmmk5NSbm6IQJmhposdCtDzjXRFgzUvKrgFsomJggYUCOYIaMzPtsVf4GKkmz.kA5hSCu';
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

async function getLastBackupTime(objectName) {
  const client = new Client(PG_CONFIG);
  await client.connect();
  try {
    const res = await client.query('SELECT last_run FROM last_backup WHERE object_name = $1', [objectName]);
    return res.rows[0]?.last_run || null;
  } finally {
    await client.end();
  }
}

async function setLastBackupTime(objectName) {
  const client = new Client(PG_CONFIG);
  await client.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS last_backup (
        object_name TEXT PRIMARY KEY,
        last_run TIMESTAMP
      );
      INSERT INTO last_backup (object_name, last_run)
      VALUES ($1, NOW())
      ON CONFLICT (object_name)
      DO UPDATE SET last_run = NOW();
    `, [objectName]);
  } finally {
    await client.end();
  }
}

async function hasRecentChangesSince(objectName, sinceTimestamp) {
  const modField = 'LastModifiedDate';
  const formatted = sinceTimestamp.toISOString();
  const q = encodeURIComponent(`SELECT Id FROM ${objectName} WHERE ${modField} > ${formatted} LIMIT 1`);
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/query?q=${q}`;
  try {
    const res = await axios.get(url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });
    return res.data.totalSize > 0;
  } catch (e) {
    console.warn(`⚠️ Error checking recent changes for ${objectName}: ${e.message}`);
    return false;
  }
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

async function insertCSVToPostgres(filePath, objectName) {
  const client = new Client(PG_CONFIG);
  await client.connect();

  const tempTable = `"${objectName}_temp"`;

  try {
    const headers = await new Promise((resolve, reject) => {
      fs.createReadStream(filePath).pipe(csv()).on('headers', resolve).on('error', reject);
    });

    await client.query(`CREATE TABLE IF NOT EXISTS "${objectName}" (${headers.map(h => `"${h}" ${mapSFTypeToPostgres(FIELD_TYPES_MAP[h] || 'string')}`).join(', ')});`);
    await client.query(`DROP TABLE IF EXISTS ${tempTable};`);
    await client.query(`CREATE TEMP TABLE ${tempTable} AS SELECT * FROM "${objectName}" WITH NO DATA;`);

    const copySQL = `COPY ${tempTable} (${headers.map(h => `"${h}"`).join(', ')}) FROM STDIN WITH CSV HEADER NULL '\\N'`;
    const stream = client.query(copyFrom(copySQL));
    fs.createReadStream(filePath).pipe(stream);
    await new Promise((resolve, reject) => {
      stream.on('finish', resolve);
      stream.on('error', reject);
    });

    const upsertSQL = `
      INSERT INTO "${objectName}" (${headers.map(h => `"${h}"`).join(', ')})
      SELECT ${headers.map(h => `"${h}"`).join(', ')} FROM ${tempTable}
      ON CONFLICT ("Id") DO UPDATE SET
      ${headers.filter(h => h !== 'Id').map(h => `"${h}" = EXCLUDED."${h}"`).join(', ')};
    `;
    await client.query(upsertSQL);

    const result = await client.query(`SELECT COUNT(*) FROM ${tempTable};`);
    return parseInt(result.rows[0].count, 10);
  } finally {
    await client.end();
  }
}

async function backupObject(objectName, isIncremental = false) {
  let rawPath, cleanPath;
  try {
    const fields = await getAllFields(objectName);
    let soql = `SELECT ${fields.join(', ')} FROM ${objectName}`;

    if (isIncremental) {
      const lastTime = await getLastBackupTime(objectName);
      if (lastTime && fields.includes('LastModifiedDate')) {
        const timestamp = lastTime.toISOString().replace('T', ' ').slice(0, 19);
        soql += ` WHERE LastModifiedDate > ${timestamp}Z`;
        const changed = await hasRecentChangesSince(objectName, lastTime);
        if (!changed) {
          console.log(`⏭️ Skipped (no changes): ${objectName}`);
          return;
        }
      }
    }

    const job = await createBulkQueryJob(soql);
    await pollJob(job.id);
    rawPath = await downloadResults(job.id);
    cleanPath = await cleanCSV(rawPath);

    const recordCount = await insertCSVToPostgres(cleanPath, objectName);
    await setLastBackupTime(objectName);

    console.log(`✅ Success: ${objectName} (${recordCount} records)`);
  } catch (err) {
    console.warn(`❌ Failed ${objectName}: ${err.message}`);
  } finally {
    if (rawPath && fs.existsSync(rawPath)) fs.unlinkSync(rawPath);
    if (cleanPath && fs.existsSync(cleanPath)) fs.unlinkSync(cleanPath);
  }
}

// === Express Server ===
const app = express();
app.use(express.json());

app.post('/api/backup', async (req, res) => {
  try {
    const inputList = req.body.objectNames || [];
    const isIncremental = req.body.incremental || false;

    const allObjects = await getAllObjectNames();
    const selected = inputList.length ? allObjects.filter(o => inputList.includes(o)) : allObjects;

    res.json({ message: `✅ ${isIncremental ? 'Incremental' : 'Full'} backup started`, objects: selected });

    process.nextTick(async () => {
      const limit = pLimit(1);
      for (let obj of selected) {
        await limit(() => backupObject(obj, isIncremental));
      }
    });
  } catch (err) {
    console.error('❌ API Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
