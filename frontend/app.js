const express = require('express');
const mysql = require('mysql2/promise');
const AWS = require('aws-sdk');

const app = express();
const port = 3000;

// Konfigurasi AWS SSM
const ssm = new AWS.SSM({ region: 'us-east-1' }); // Ganti region sesuai kebutuhan

async function getSSMParameter(name) {
    const params = { Name: name, WithDecryption: true };
    const result = await ssm.getParameter(params).promise();
    return result.Parameter.Value;
}

async function getDBConfig() {
    const [host, user, password, database, table] = await Promise.all([
        getSSMParameter('/tryout1/RDSHost'),
        getSSMParameter('/tryout1/RDSUsername'),
        getSSMParameter('/tryout1/RDSPassword'),
        getSSMParameter('/tryout1/RDSDatabase'),
        getSSMParameter('/tryout1/RDSTable')
    ]);

    return { host, user, password, database, table };
}

app.get('/', async (req, res) => {
    try {
        const dbConfig = await getDBConfig();
        const connection = await mysql.createConnection({
            host: dbConfig.host,
            user: dbConfig.user,
            password: dbConfig.password,
            database: dbConfig.database
        });

        const [rows] = await connection.execute(`SELECT * FROM ${dbConfig.table} ORDER BY id DESC`);
        await connection.end();

        let html = '<h2>Data Event</h2><table border="1"><tr><th>ID</th><th>Device ID</th><th>Event Type</th><th>Value</th><th>Timestamp</th></tr>';
        rows.forEach(row => {
            html += `<tr><td>${row.id}</td><td>${row.device_id}</td><td>${row.event_type}</td><td>${row.value}</td><td>${row.timestamp}</td></tr>`;
        });
        html += '</table>';

        res.send(html);
    } catch (error) {
        res.status(500).send('Error: ' + error.message);
    }
});

app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});
