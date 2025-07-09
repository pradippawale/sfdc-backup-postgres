const axios = require('axios');
const fs = require('fs');
const path = require('path');
const { Client } = require('pg');
const csv = require('csv-parser');
const copyFrom = require('pg-copy-streams').from;
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify');
const pLimit = require('p-limit').default; // ✅ FIX: no `.default`

// === CONFIG ===
const ACCESS_TOKEN = '00DfJ000002QrbH!AQEAQF11EjB.Nx5.rHXQ.V_sRksazBynBhfC8ZbWl1S1aG0gNez_MutGGqYWA1om43NswEtuBgljHJgp1uwErLd_SlNgFB_i';
const INSTANCE_URL = 'https://coresolute4-dev-ed.develop.my.salesforce.com';
const API_VERSION = 'v60.0';

const PG_CONFIG = {
    host: 'dpg-d1i3u8fdiees73cf0dug-a.oregon-postgres.render.com',
    port: 5432,
    database: 'sfdatabase_34oi',
    user: 'sfdatabaseuser',
    password: 'D898TUsAal4ksBUs5QoQffxMZ6MY5aAH',
    ssl: { rejectUnauthorized: false }
};

const delay = ms => new Promise(res => setTimeout(res, ms));
let FIELD_TYPES_MAP = {};

// === SFDC HELPERS ===
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
    while (state === 'InProgress' || state === 'UploadComplete') {
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

// ✅ FIX: safe parsing
async function cleanCSV(filePath) {
    const outputPath = filePath.replace('.csv', '_clean.csv');
    const parser = fs.createReadStream(filePath).pipe(parse({
        columns: true,
        relax_quotes: true,
        relax_column_count: true,
        skip_empty_lines: true,
        trim: true
    }));

    const writer = fs.createWriteStream(outputPath);
    const stringifier = stringify({ header: true });

    parser.on('data', row => {
        for (let key in row) if (row[key] === '') row[key] = null;
        stringifier.write(row);
    });

    parser.on('end', () => stringifier.end());
    stringifier.pipe(writer);

    return new Promise((resolve, reject) => {
        writer.on('finish', () => resolve(outputPath));
        writer.on('error', reject);
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

async function createTable(client, objectName, headers) {
    const cols = headers.map(h => `"${h}" ${mapSFTypeToPostgres(FIELD_TYPES_MAP[h] || 'string')}`);
    if (headers.includes('Id')) cols[headers.indexOf('Id')] += ' PRIMARY KEY';
    await client.query(`CREATE TABLE IF NOT EXISTS "${objectName}" (${cols.join(', ')});`);
}

async function insertCSVToPostgres(filePath, objectName) {
    const client = new Client(PG_CONFIG);
    await client.connect();

    const headers = await new Promise((resolve, reject) => {
        fs.createReadStream(filePath).pipe(csv()).on('headers', resolve).on('error', reject);
    });

    await createTable(client, objectName, headers);

    const copySQL = `COPY "${objectName}" (${headers.map(h => `"${h}"`).join(', ')}) FROM STDIN WITH CSV HEADER`;
    const stream = client.query(copyFrom(copySQL));
    fs.createReadStream(filePath).pipe(stream);

    await new Promise((resolve, reject) => {
        stream.on('finish', resolve);
        stream.on('error', reject);
    });

    await client.end();
    return headers.length;
}

async function logBackup({ objectName, recordCount, status, error }) {
    const url = `${INSTANCE_URL}/services/data/${API_VERSION}/sobjects/Backup_Log__c`;
    const body = {
        Object_Name__c: objectName,
        Record_Count__c: recordCount,
        Status__c: status,
        Backup_Timestamp__c: new Date().toISOString(),
        Error_Message__c: error || null
    };

    await axios.post(url, body, {
        headers: {
            Authorization: `Bearer ${ACCESS_TOKEN}`,
            'Content-Type': 'application/json'
        }
    });
}

// === MAIN BACKUP PROCESS ===
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

        try {
            const count = await insertCSVToPostgres(cleanPath, objectName);
            await logBackup({ objectName, recordCount: count, status: 'Success' });
            fs.unlinkSync(rawPath); // Delete only after success
            fs.unlinkSync(cleanPath);
            console.log(`✅ Success: ${objectName}`);
        } catch (insertErr) {
            console.error(`⚠️ Insert failed: ${objectName}, CSV kept`);
            await logBackup({ objectName, recordCount: 0, status: 'Failed', error: insertErr.message });
        }
    } catch (err) {
        console.warn(`❌ Failed: ${objectName}`, err.message);
        await logBackup({ objectName, recordCount: 0, status: 'Failed', error: err.message });
    }
}

// === Run All with Concurrency Limit ===
(async () => {
    try {
        const objectNames = await getAllObjectNames();
        const limit = pLimit(4); // Max 4 objects in parallel

        await Promise.all(objectNames.map(name => limit(() => backupObject(name))));
        console.log('\n🎉 All backups completed!');
    } catch (err) {
        console.error('❌ Fatal Error:', err.message);
    }
})();
