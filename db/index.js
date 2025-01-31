const { Pool } = require('pg');
const dbConfig = require('./config');

const pool = new Pool(dbConfig);

pool.on('connect', () => {
  console.log('Connected to the Postgres database.');
});

pool.on('error', (err) => {
  console.error('Unexpected error on Postgres client', err);
});

module.exports = pool;
