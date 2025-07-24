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
const ACCESS_TOKEN = '00DRL000003J3LR!AQEAQPtmFQRjwFveCTL96mETRkIJR6qijhuUkrk56jNy0ECTxEFlsn29Q8t28E53fnF0eRk32wRmMmQOSSOBVEE0myB9hZQp';
const INSTANCE_URL = 'https://coresolute--coredev1.sandbox.my.salesforce.com';
const API_VERSION = 'v60.0';

const PG_CONFIG = {
  host: 'sfbackup.chgsy0sgmkl1.eu-north-1.rds.amazonaws.com',
    port: 5432,
  database: 'postgres', // Or the name you created
  user: 'sfbackup_user',
  password: 'Smile2050',
  ssl: false, // or set to { rejectUnauthorized: false } for RDS with SSL
  keepAlive: true
};

// === Helper Functions ===
async function getAllObjectNames() {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/sobjects`;
  const res = await axios.get(url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });
   return res.data.sobjects
  .filter(o => o.queryable)
  .map(o => o.name);
}

async function getAllFields(objectName) {
    try {
        const url = `${INSTANCE_URL}/services/data/${API_VERSION}/sobjects/${objectName}/describe`;
        const res = await axios.get(url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });

        FIELD_TYPES_MAP = {};
		        const unsupported = ['address', 'location', 'base64', 'json'];

        let fields = res.data.fields
            .filter(f =>
                !unsupported.includes(f.type) &&
                (!f.compoundFieldName || f.name === 'Name')
            )
            .map(f => {
                FIELD_TYPES_MAP[f.name] = f.type;
                return f.name;
            });

        if (!fields.includes('Id')) {
            fields.unshift('Id');
            FIELD_TYPES_MAP['Id'] = 'id';
        }

        const hasName = res.data.fields.find(f => f.name === 'Name');
		        if (hasName && !fields.includes('Name')) {
            fields.unshift('Name');
            FIELD_TYPES_MAP['Name'] = hasName.type;
        }

        return [...new Set(fields)];
    } catch (err) {
        console.warn(`⚠️FFailed to get fields for ${objectName}: ${err.message}`);
        return [];
    }
}

async function getLastBackupTime(objectName) {
  const client = new Client(PG_CONFIG);
  await client.connect();
  try {
    await client.query(`CREATE TABLE IF NOT EXISTS last_backup (object_name TEXT PRIMARY KEY, last_run TIMESTAMP);`);
    const res = await client.query('SELECT last_run FROM last_backup WHERE object_name = $1', [objectName]);
    return res.rows[0]?.last_run || new Date('2000-01-01');
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
      ON CONFLICT (object_name) DO UPDATE SET last_run = NOW();
    `, [objectName]);
  } finally {
    await client.end();
  }
}

async function hasRecentChangesSince(objectName, sinceTimestamp) {
  const q = encodeURIComponent(`SELECT Id FROM ${objectName} WHERE LastModifiedDate > ${sinceTimestamp.toISOString()} LIMIT 1`);
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

async function pollJob(jobId) {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/jobs/query/${jobId}`;
  let state = 'InProgress';
  const start = Date.now();
  while (['InProgress', 'UploadComplete'].includes(state)) {
    if (Date.now() - start > 300000) throw new Error(`Timeout on job ${jobId}`);
    const res = await axios.get(url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });
    state = res.data.state;
	    if (state === 'JobComplete') return;
    if (['Failed', 'Aborted'].includes(state)) throw new Error(`Job ${state}`);
    await delay(3000);
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
  const headers = await new Promise((resolve, reject) => {
    fs.createReadStream(cleanPath).pipe(csv()).on('headers', resolve).on('error', reject);
  });

  // Ensure table exists with unique constraint
  await client.query(`
    CREATE TABLE IF NOT EXISTS "${objectName}" (
	      ${headers.map(h => `"${h}" TEXT`).join(', ')},
      UNIQUE ("Id")
    );
  `);

  const tempTable = `"${objectName}_temp"`;
  await client.query(`DROP TABLE IF EXISTS ${tempTable}`);
  await client.query(`CREATE TEMP TABLE ${tempTable} AS SELECT * FROM "${objectName}" WITH NO DATA`);

  const copySQL = `COPY ${tempTable} (${headers.map(h => `"${h}"`).join(', ')}) FROM STDIN WITH CSV HEADER NULL '\\N'`;
  const stream = client.query(copyFrom(copySQL));
  fs.createReadStream(cleanPath).pipe(stream);
  await new Promise((resolve, reject) => {
    stream.on('finish', resolve);
    stream.on('error', reject);
  });

  const upsertSQL = `
  INSERT INTO "${objectName}" (${headers.map(h => `"${h}"`).join(', ')})
  SELECT DISTINCT ON ("Id") ${headers.map(h => `"${h}"`).join(', ')} FROM ${tempTable}
  ON CONFLICT ("Id") DO UPDATE SET
  ${headers.filter(h => h !== 'Id').map(h => `"${h}" = EXCLUDED."${h}"`).join(', ')};
`;


  await client.query(upsertSQL);
  const res = await client.query(`SELECT COUNT(*) FROM ${tempTable}`);
  await client.end();
  return parseInt(res.rows[0].count);
}

// 🔁 Main Backup Logic
async function backupObject(objectName, mode = 'incremental', dateStr, conn) {
  let rawPath, cleanPath;
  console.time(`🔁 ${objectName}`);

  try {
    const fields = await getAllFields(objectName);
    const hasLastModified = fields.includes('LastModifiedDate');
    const lastTime = await getLastBackupTime(objectName);

    // Step 1: Check if incremental and skip if no recent changes
    if (mode === 'incremental') {
      const hasChanges = hasLastModified
        ? await hasRecentChangesSince(objectName, lastTime)
		        : true; // fallback to true if no LastModifiedDate

      if (!hasChanges) {
        await logBackupToSalesforce({
          objectName,
          recordCount: 0,
          status: 'Skipped',
          error: 'No recent changes found.'
        });
        console.log(`⏭️ Skipped ${objectName} (no recent changes)`);
        return;
      }
    }
	
    // Step 2: Build SOQL
    let soql;
    if (mode === 'incremental' && hasLastModified) {
      soql = `SELECT ${fields.join(', ')} FROM ${objectName} WHERE LastModifiedDate > ${lastTime.toISOString()}`;
    } else if (mode === 'full' && hasLastModified) {
      soql = `SELECT ${fields.join(', ')} FROM ${objectName} WHERE LastModifiedDate >= ${dateStr}`;
    } else {
      soql = `SELECT ${fields.join(', ')} FROM ${objectName}`;
    }

    // Step 3: Run SOQL via Bulk API
    let job;
    try {
      job = await createBulkQueryJob(soql);
      await pollJob(job.id);
    } catch (jobErr) {
      const status = jobErr.response?.status;
      const data = jobErr.response?.data;
      console.error(`❌ ${objectName} SOQL job failed`);
      console.error(`Status: ${status}`);
      console.error(`Response:`, JSON.stringify(data, null, 2));
      console.error(`Error: ${jobErr.message}`);
	  
      await logBackupToSalesforce({
        objectName,
        recordCount: 0,
        status: 'Failed',
        error: `Bulk query job failed: ${jobErr.message}`
      });
      return;
    }

    // Step 4: Download and clean CSV
    rawPath = await downloadResults(job.id);
    cleanPath = await cleanCSV(rawPath);

    // Step 5: Check if data has more than header
    const lineCount = await new Promise((resolve, reject) => {
      let count = 0;
      fs.createReadStream(cleanPath)
        .on('data', chunk => {
          count += chunk.toString().split('\n').length - 1;
        })
        .on('end', () => resolve(count))
        .on('error', reject);
		    });

    if (lineCount <= 1) {
      await logBackupToSalesforce({
        objectName,
        recordCount: 0,
        status: 'Skipped',
        error: 'No records found for backup.',
        csvFilePath: cleanPath
      });
      console.log(`⏭️ Skipped ${objectName} (no records)`);
      return;
    }

    // Step 6: Insert to Postgres
    const count = await insertToPostgres(cleanPath, objectName);
    await setLastBackupTime(objectName);

    console.log(`✅ ${objectName}: ${count} records backed up.`);
    await logBackupToSalesforce({
      objectName,
      recordCount: count,
	        status: 'Success',
      csvFilePath: cleanPath
    });

  } catch (err) {
    const status = err.response?.status;
    const data = err.response?.data;
    console.error(`❌ ${objectName} backup failed`);
    console.error(`Status: ${status}`);
    console.error(`Response:`, JSON.stringify(data, null, 2));
    console.error(`Error: ${err.message}`);
    console.error(err.stack);

    await logBackupToSalesforce({
      objectName,
      recordCount: 0,
      status: 'Failed',
	        error: err.message
    });

  } finally {
    if (rawPath && fs.existsSync(rawPath)) fs.unlinkSync(rawPath);
    if (cleanPath && fs.existsSync(cleanPath)) fs.unlinkSync(cleanPath);
    console.timeEnd(`🔁 ${objectName}`);
        }
}

//Log Backup to salesforce
async function logBackupToSalesforce({ objectName, recordCount, status, error = '', csvFilePath = '' }) {
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

    if (status === 'Success' && csvFilePath && fs.existsSync(csvFilePath)) {
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
        {
          headers: { Authorization: `Bearer ${ACCESS_TOKEN}` }
        }
      );

      const contentDocumentId = queryRes.data.records?.[0]?.ContentDocumentId;

      if (contentDocumentId) {
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
        console.log(`📎 Attached file to Backup Log ${backupLogId}`);
      }
    }

    console.log(`📝 Backup log created for ${objectName}`);
  } catch (err) {
    const status = err.response?.status;
    const data = err.response?.data;
	
    console.error(`⚠️ Failed to log backup for ${objectName}`);
    console.error(`Status Code: ${status}`);
    console.error(`Response Body:`, JSON.stringify(data, null, 2));
    console.error(`Stack:`, err.stack);
  }
}





// === Express Server ===
const app = express();
app.use(express.json());

app.post('/api/backup', async (req, res) => {
  const list = req.body.objectNames || [];
  const mode = req.body.mode === 'full' ? 'full' : 'incremental';

  const allObjects = await getAllObjectNames();
  const toBackup = list.length ? allObjects.filter(o => list.includes(o)) : allObjects;
  
  // Respond immediately
  res.json({ message: `🔄 ${mode} backup started...`, objects: toBackup });

  // Run backup in background using batches
  process.nextTick(async () => {
    const BATCH_SIZE = 200;
    const batches = [];
    for (let i = 0; i < toBackup.length; i += BATCH_SIZE) {
      batches.push(toBackup.slice(i, i + BATCH_SIZE));
    }

    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batch = batches[batchIndex];
      console.log(`🚀 Starting batch ${batchIndex + 1}/${batches.length} (${batch.length} objects)`);

      const limit = pLimit(5); // still process max 5 in parallel
          const dateStr = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString(); // 30 days ago
      const conn = { query: () => { throw new Error("conn.query not implemented"); } }; // 🔧 Replace with actual conn object if needed
      const tasks = batch.map(obj => limit(async () => {
  const TIMEOUT_MS = 2 * 60 * 1000; // 2 minutes
  try {
    await Promise.race([
      backupObject(obj, mode, dateStr, conn),
      new Promise((_, reject) => setTimeout(() => reject(new Error(`Timeout after 2 minutes`)), TIMEOUT_MS))
    ]);
  } catch (err) {
    console.error(`⏳ Timeout or failure on ${obj}: ${err.message}`);
    await logBackupToSalesforce({
      objectName: obj,
      recordCount: 0,
      status: 'Failed',
      error: `Timeout or unexpected error: ${err.message}`
    });
  }
}));
await Promise.all(tasks);


      console.log(`✅ Finished batch ${batchIndex + 1}\n`);
	      }
    console.log('🎉 All object backups completed.');
  });
});


app.listen(3000, '0.0.0.0', () => {
  console.log('🚀 Server on port 3000');
});
