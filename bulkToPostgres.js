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

function chunkArray(array, size) {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

// === Salesforce Helpers ===
async function getAllObjectNames() {
  const url = `${INSTANCE_URL}/services/data/${API_VERSION}/sobjects`;
  const res = await axios.get(url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });
  return res.data.sobjects
    .filter(o => o.queryable && !o.name.endsWith('__Share') && !o.name.endsWith('__Tag') && !o.name.endsWith('__History') && !o.name.endsWith('__Feed') && !o.name.includes('ChangeEvent') && !o.name.toLowerCase().includes('permissionsetgroup') && !o.name.toLowerCase().includes('recordtype'))
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
    for (let key in row) {
      const value = row[key];
      row[key] = value === '' ? '\\N' : value;
    }
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

function mapSFTypeToPostgres(sfType) {
  const map = {
    string: 'TEXT', picklist: 'TEXT', textarea: 'TEXT', email: 'TEXT', id: 'TEXT',
    phone: 'TEXT', url: 'TEXT', boolean: 'BOOLEAN', int: 'INTEGER',
    double: 'FLOAT', currency: 'FLOAT', percent: 'FLOAT', date: 'DATE', datetime: 'TIMESTAMP'
  };
  return map[sfType.toLowerCase()] || 'TEXT';
}

async function insertCSVToPostgres(filePath, objectName) {
  const client = new Client(PG_CONFIG);
  await client.connect();

  let rowCount = 0;

  try {
    const headers = await new Promise((resolve, reject) => {
      fs.createReadStream(filePath).pipe(csv()).on('headers', resolve).on('error', reject);
    });

    const cols = headers.map(h => `"${h}" ${mapSFTypeToPostgres(FIELD_TYPES_MAP[h] || 'string')}`);
    if (headers.includes('Id')) cols[headers.indexOf('Id')] += ' PRIMARY KEY';
    await client.query(`CREATE TABLE IF NOT EXISTS "${objectName}" (${cols.join(', ')});`);

    const copySQL = `COPY "${objectName}" (${headers.map(h => `"${h}"`).join(', ')}) FROM STDIN WITH CSV HEADER NULL '\\N'`;
    const stream = client.query(copyFrom(copySQL));
    fs.createReadStream(filePath).pipe(stream);

    await new Promise((resolve, reject) => {
      stream.on('finish', resolve);
      stream.on('error', reject);
    });

    rowCount = await new Promise((resolve, reject) => {
      let count = 0;
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', () => count++)
        .on('end', () => resolve(count))
        .on('error', reject);
    });

    return rowCount;
  } finally {
    await client.end();
  }
}

// === Main execution block ===
(async () => {
  try {
    const objectNames = await getAllObjectNames();
    console.log(`📦 Total objects to backup: ${objectNames.length}`);

    for (let i = 0; i < objectNames.length; i++) {
      const objectName = objectNames[i];
      console.log(`🔄 (${i + 1}/${objectNames.length}) Processing ${objectName}`);

      const hasData = await hasRecords(objectName);
      if (!hasData) {
        console.log(`⏭️ Skipped: ${objectName}`);
        continue;
      }

      const fields = await getAllFields(objectName);
      const soql = `SELECT ${fields.join(', ')} FROM ${objectName}`;

      const job = await createBulkQueryJob(soql);
      await pollJob(job.id);
      const rawPath = await downloadResults(job.id);
      const cleanPath = await cleanCSV(rawPath);

      const count = await insertCSVToPostgres(cleanPath, objectName);
      console.log(`✅ ${objectName} inserted (${count} records)`);

      fs.unlinkSync(rawPath);
      fs.unlinkSync(cleanPath);
    }

    console.log('🎉 All object backups completed.');
  } catch (err) {
    console.error('❌ Fatal error:', err.message);
  }
})();
