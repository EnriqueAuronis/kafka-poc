
const { Kafka } = require('kafkajs');

let producer;

async function initKafkaProducer() {
  if (!producer) {
    const kafka = new Kafka({
        clientId: process.env.KAFKA_CLIENT_ID || "my-kafka-consumer",
        brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
        connectionTimeout: 3000,
        retry: {
            initialRetryTime: 100,
            retries: 8,
        },
      });
    producer = kafka.producer();
    await producer.connect();
    console.log('Kafka Producer connected successfully.');
  }
  return producer;
}

function getProducer() {
  if (!producer) {
    throw new Error('Producer not initialized. Call initKafkaProducer() first.');
  }
  return producer;
}

module.exports = { initKafkaProducer, getProducer };
