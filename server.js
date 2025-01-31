// server.js
require("dotenv").config();
const cluster = require("cluster");
const os = require("os");
const express = require("express");
const { createBusAccessTable } = require("./db/createTable");
const { sendAccess } = require("./producer/sendBusAccessKafka");
const { initKafkaProducer } = require("./config/kafka");

const numCPUs = os.cpus().length;
const PORT = process.env.PORT || 3000;

if (cluster.isPrimary) {
  console.log(`Master ${process.pid} is running`);

  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on("exit", (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died`);
  });
} else {
  console.log(`Worker ${process.pid} started`);

  const app = express();
  app.use(express.json());

  createBusAccessTable()
    .then(() => console.log("bus_access table ensured"))
    .catch((err) => console.error("Error creating table:", err));

  initKafkaProducer()
    .then(() => {
      app.post("/app/sendAccess", sendAccess);
      app.listen(PORT, () => {
        console.log(`Worker ${process.pid} listening on port ${PORT}...`);
      });
    })
    .catch((err) => {
      console.error("Error connecting Kafka Producer:", err);
      process.exit(1);
    });
}
