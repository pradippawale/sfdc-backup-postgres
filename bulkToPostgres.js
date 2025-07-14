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

// === Config ===
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
      !o.name.match(/(__Share|__Tag|__History|__Feed|ChangeEvent)/) &&
      !o.name.toLowerCase().includes('permissionsetgroup') &&
      !o.name.toLowerCase().includes('recordtype')
    )
    .map(o => o.name);
}

async function getAllFields(objectName) {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/sobjects/${objectName}/describe`;
  const res = await axios.get(url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });

  FIELD_TYPES_MAP = {};
  const unsupported = ['address', 'location', 'base64', 'json'];
  const fields = res.data.fields
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
    await client.query(`CREATE TABLE IF NOT EXISTS last_backup (object_name TEXT PRIMARY KEY, last_run TIMESTAMP);`);
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
  const formatted = sinceTimestamp.toISOString();
  const q = encodeURIComponent(`SELECT Id FROM ${objectName} WHERE LastModifiedDate > ${formatted} LIMIT 1`);
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/query?q=${q}`;
  try {
    const res = await axios.get(url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });
    return res.data.totalSize > 0;
  } catch {
    return false;
  }
}

async function createBulkQueryJob(soql) {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/jobs/query`;
  const res = await axios.post(url, {
    operation: 'query',
    query: soql,
    contentType: 'CSV'
  }, {
    headers: { Authorization: `Bearer ${ACCESS_TOKEN}`, 'Content-Type': 'application/json' }
  });
  return res.data;
}

async function pollJob(jobId, timeout = 300000) {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/jobs/query/${jobId}`;
  let state = 'InProgress';
  const start = Date.now();
  while (state === 'InProgress' || state === 'UploadComplete') {
    if (Date.now() - start > timeout) throw new Error(`Polling timeout for job ${jobId}`);
    const res = await axios.get(url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });
    state = res.data.state;
    if (state === 'JobComplete') return;
    if (["Failed", "Aborted"].includes(state)) throw new Error(`Job ${state}`);
    await delay(4000);
  }
}

async function downloadResults(jobId) {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/jobs/query/${jobId}/results`;
  const res = await axios.get(url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` }, responseType: 'stream' });
  const filePath = path.join(__dirname, `temp_${jobId}.csv`);
  const writer = fs.createWriteStream(filePath);
  res.data.pipe(writer);
  return new Promise((resolve, reject) => {
    writer.on('finish', () => resolve(filePath));
    writer.on('error', reject);
  });
}

async function cleanCSV(filePath) {
  const outputPath = filePath.replace('.csv', '_clean.csv');
  const parser = fs.createReadStream(filePath).pipe(parse({ columns: true, skip_empty_lines: true }));
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

async function insertToPostgres(cleanPath, objectName) {
  const client = new Client(PG_CONFIG);
  await client.connect();

  try {
    const headers = await new Promise((resolve, reject) => {
      fs.createReadStream(cleanPath).pipe(csv())
        .on('headers', resolve)
        .on('error', reject);
    });

    // Check if table exists
    const tableExistsCheck = await client.query(
      `SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = $1
      )`, [objectName.toLowerCase()]
    );

    const tableExists = tableExistsCheck.rows[0].exists;

    // If table doesn't exist, create it with PRIMARY KEY on Id
    if (!tableExists) {
      const columnsDef = headers.map(h => `"${h}" TEXT`).join(', ');
      const pkConstraint = headers.includes('Id') ? ', PRIMARY KEY ("Id")' : '';
      const createSQL = `CREATE TABLE "${objectName}" (${columnsDef}${pkConstraint});`;
      await client.query(createSQL);
    } else {
      // Ensure PRIMARY KEY exists on Id
      if (headers.includes('Id')) {
        const pkCheck = await client.query(`
          SELECT constraint_type 
          FROM information_schema.table_constraints 
          WHERE table_name = $1 AND constraint_type = 'PRIMARY KEY'
        `, [objectName]);
        if (pkCheck.rows.length === 0) {
          try {
            await client.query(`ALTER TABLE "${objectName}" ADD PRIMARY KEY ("Id")`);
          } catch (err) {
            console.warn(`⚠️ Couldn't add PRIMARY KEY to ${objectName}. It may already exist or be invalid:`, err.message);
          }
        }
      }
    }

    // Temp table
    const tempTable = `"${objectName}_temp"`;
    await client.query(`DROP TABLE IF EXISTS ${tempTable}`);
    await client.query(`CREATE TEMP TABLE ${tempTable} AS SELECT * FROM "${objectName}" WITH NO DATA`);

    // COPY into temp table
    const copySQL = `COPY ${tempTable} (${headers.map(h => `"${h}"`).join(', ')}) FROM STDIN WITH CSV HEADER NULL '\\N'`;
    const stream = client.query(copyFrom(copySQL));
    fs.createReadStream(cleanPath).pipe(stream);
    await new Promise((resolve, reject) => {
      stream.on('finish', resolve);
      stream.on('error', reject);
    });

    // UPSERT using ON CONFLICT on Id
    const upsertSQL = `
      INSERT INTO "${objectName}" (${headers.map(h => `"${h}"`).join(', ')})
      SELECT ${headers.map(h => `"${h}"`).join(', ')} FROM ${tempTable}
      ON CONFLICT ("Id") DO UPDATE SET
      ${headers.filter(h => h !== 'Id').map(h => `"${h}" = EXCLUDED."${h}"`).join(', ')};
    `;
    await client.query(upsertSQL);

    const countRes = await client.query(`SELECT COUNT(*) FROM ${tempTable}`);
    return parseInt(countRes.rows[0].count);
  } finally {
    await client.end();
  }
}

async function logBackupToSalesforce(objectName, recordCount, status, cleanPath = null) {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/sobjects/Backup_Log__c`;
  const body = {
    Object_Name__c: objectName,
    Record_Count__c: recordCount,
    Status__c: status,
    Backup_Timestamp__c: new Date().toISOString()
  };
  const res = await axios.post(url, body, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}`, 'Content-Type': 'application/json' } });
  const logId = res.data.id;

  if (cleanPath) {
    const fileData = fs.readFileSync(cleanPath).toString('base64');
    const fileName = path.basename(cleanPath);
    const contentVersion = await axios.post(
      `${INSTANCE_URL}/services/data/${API_VERSION}/sobjects/ContentVersion`,
      {
        Title: `${objectName}_Backup`,
        PathOnClient: fileName,
        VersionData: fileData
      },
      { headers: { Authorization: `Bearer ${ACCESS_TOKEN}`, 'Content-Type': 'application/json' } }
    );
    const versionId = contentVersion.data.id;
    const contentDocRes = await axios.get(
      `${INSTANCE_URL}/services/data/${API_VERSION}/query?q=` +
      encodeURIComponent(`SELECT ContentDocumentId FROM ContentVersion WHERE Id = '${versionId}'`),
      { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } }
    );
    const contentDocumentId = contentDocRes.data.records[0].ContentDocumentId;
    await axios.post(
      `${INSTANCE_URL}/services/data/${API_VERSION}/sobjects/ContentDocumentLink`,
      {
        ContentDocumentId: contentDocumentId,
        LinkedEntityId: logId,
        ShareType: 'V'
      },
      { headers: { Authorization: `Bearer ${ACCESS_TOKEN}`, 'Content-Type': 'application/json' } }
    );
  }
}

async function backupObject(objectName) {
  let rawPath, cleanPath;
  try {
    const fields = await getAllFields(objectName);
    if (!fields.includes('LastModifiedDate')) return; // ⛔ Skip if no LastModifiedDate
    const lastTime = await getLastBackupTime(objectName);
    if (!lastTime) return;
    const hasChanges = await hasRecentChangesSince(objectName, lastTime);
    if (!hasChanges) return;

    const soql = `SELECT ${fields.join(', ')} FROM ${objectName} WHERE LastModifiedDate > ${lastTime.toISOString()}`;
    const job = await createBulkQueryJob(soql);
    await pollJob(job.id);
    rawPath = await downloadResults(job.id);
    cleanPath = await cleanCSV(rawPath);
    const count = await insertToPostgres(cleanPath, objectName);
    await logBackupToSalesforce(objectName, count, 'Success', cleanPath);
    await setLastBackupTime(objectName);
  } catch (e) {
    await logBackupToSalesforce(objectName, 0, 'Failed');
    console.error(`❌ ${objectName}: ${e.message}`);
  } finally {
    if (rawPath && fs.existsSync(rawPath)) fs.unlinkSync(rawPath);
    if (cleanPath && fs.existsSync(cleanPath)) fs.unlinkSync(cleanPath);
  }
}

// === Express Server ===
const app = express();
app.use(express.json());

app.post('/api/backup', async (req, res) => {
  const list = req.body.objectNames || [];
  const all = await getAllObjectNames();
  const toBackup = list.length ? all.filter(o => list.includes(o)) : all;
  res.json({ message: '🔄 Incremental backup started...', objects: toBackup });
  process.nextTick(async () => {
    const limit = pLimit(1);
    for (const obj of toBackup) await limit(() => backupObject(obj));
  });
});

app.listen(3000, () => console.log('🚀 Server on port 3000'));
