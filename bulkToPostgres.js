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

const delay = ms => new Promise(res => setTimeout(res, ms));
let FIELD_TYPES_MAP = {};

// === Configuration ===
const ACCESS_TOKEN = '00DfJ000002QrbH!AQEAQEaZ8jPe73YZ3z4eTm1C8XqVkbDUnP5RLMWBW.Hhlm9Nog9z83W.stfFm3oa7aoJVAt1NkxBf8D3_IUYjBB.3NH3juNk';
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

// === Salesforce Helpers ===
async function getAllObjectNames() {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/sobjects`;
  const res = await axios.get(url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });
  return res.data.sobjects
    .filter(o => o.queryable && !o.name.endsWith('__Share') && !o.name.endsWith('__Tag'))
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

  return res.data.fields
    .filter(f => !unsupported.includes(f.type) && !f.compoundFieldName)
    .map(f => {
      FIELD_TYPES_MAP[f.name] = f.type;
      return f.name;
    });
}

// (Other helper functions such as createBulkQueryJob, pollJob, downloadResults, cleanCSV, insertCSVToPostgres remain unchanged)

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

      const queryUrl = `${INSTANCE_URL}/services/data/${API_VERSION}/query?q=` +
        encodeURIComponent(`SELECT ContentDocumentId FROM ContentVersion WHERE Id = '${contentVersionId}'`);
      const queryRes = await axios.get(queryUrl, {
        headers: { Authorization: `Bearer ${ACCESS_TOKEN}` }
      });

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
    const rawPath = await downloadResults(job.id);
    const cleanPath = await cleanCSV(rawPath);

    const recordCount = await insertCSVToPostgres(cleanPath, objectName);
    await logBackup({ objectName, recordCount, status: 'Success', csvFilePath: cleanPath });

    fs.unlinkSync(rawPath);
    fs.unlinkSync(cleanPath);
    console.log(`✅ Success: ${objectName}`);
  } catch (err) {
    console.warn(`❌ Failed: ${objectName}: ${err.message}`);
    await logBackup({ objectName, recordCount: 0, status: 'Failed', error: err.message });
  }
}

// === EXPRESS SERVER (for on-demand trigger) ===
const app = express();
app.use(express.json());

app.get('/', (_, res) => res.send('✅ Salesforce Backup Service Running.'));

app.post('/api/backup', async (req, res) => {
  try {
    const inputList = req.body.objectNames || [];
    const allObjects = await getAllObjectNames();
    const selected = inputList.length ? allObjects.filter(o => inputList.includes(o)) : allObjects;

    const limit = pLimit(1);
    for (let i = 0; i < selected.length; i++) {
      await limit(() => backupObject(selected[i]));
    }

    res.json({ message: '✅ Backup completed.', objects: selected });
  } catch (err) {
    console.error('❌ API Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
if (require.main === module) {
  app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
}
