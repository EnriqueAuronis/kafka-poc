// consumers/receiveBusAccessKafka.js
require("dotenv").config();
const { Kafka } = require("kafkajs");
const { insertBusAccess } = require("../models/busAccess");
const { saveErrorToFile } = require("./saveErrorToFile");

async function runKafkaConsumer() {
  const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID || "my-kafka-consumer",
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
    connectionTimeout: 3000,
    retry: {
      initialRetryTime: 100,
      retries: 8,
    },
  });

  const consumer = kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID || "bus-access-group",
  });

  await consumer.connect();

  await consumer.subscribe({
    topic: process.env.TOPIC_DACCESS || "dAccess",
    fromBeginning: false,
  });
  await consumer.subscribe({
    topic: process.env.TOPIC_DACCESS_NO_TRACE || "dAccessNoTraceability",
    fromBeginning: false,
  });

  console.log("Kafka consumer connected, waiting for messages...");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const contentStr = message.value.toString();
        const content = JSON.parse(contentStr);

        const success = await insertBusAccess(content);
        if (success) {
          console.log(
            "Message processed & inserted successfully. Topic:",
            topic
          );
        } else {
          console.warn("Message processed but insert failed. Topic:", topic);
        }
      } catch (err) {
        console.error("Error processing message:", err);
  
        let rawContent;
        try {
          rawContent = JSON.parse(message.value.toString());
        } catch (_) {
          rawContent = { rawPayload: message.value.toString() };
        }
  
        await saveErrorToFile(rawContent, topic, err);
      }
    },
  });
}

runKafkaConsumer().catch((err) => {
  console.error("Error starting Kafka consumer:", err);
  process.exit(1);
});
