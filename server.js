// server.js
require('dotenv').config();
const express = require('express');
const { createBusAccessTable } = require('./db/createTable');
const { sendAccess } = require('./producer/sendBusAccessKafka');
const { initKafkaProducer } = require('./config/kafka');
const app = express();

app.use(express.json());

createBusAccessTable()
  .then(() => console.log('bus_access table ensured'))
  .catch((err) => console.error('Error creating table:', err));

app.post('/app/sendAccess', sendAccess);

const PORT = process.env.PORT || 3000;
initKafkaProducer()
  .then(() => {
    app.listen(PORT, () => {
      console.log('Server listening on port 3000...');
    });
  })
  .catch(err => {
    console.error('Error connecting Kafka Producer:', err);
    process.exit(1);
  });
