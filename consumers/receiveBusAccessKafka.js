// consumers/receiveBusAccessKafka.js
require("dotenv").config();
const { Kafka } = require("kafkajs");
const { client: redisClient, connectRedis } = require("../config/redisClient");
const { insertBusAccess } = require("../models/busAccess");
const { saveErrorToFile } = require("./saveErrorToFile");

const MAX_RETRIES = 3;

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
      const messageId = message.key
        ? message.key.toString()
        : message.value.toString();

      try {
        const retryCountStr = await redisClient.get(messageId);
        const currentRetryCount = retryCountStr
          ? parseInt(retryCountStr, 10)
          : 0;

        if (currentRetryCount >= MAX_RETRIES) {
          console.warn(
            `Mensaje con ID ${messageId} alcanzó el máximo de reintentos (${MAX_RETRIES}). Se descartará.`
          );
          await redisClient.del(messageId);
          return;
        }

        const contentStr = message.value.toString();
        const content = JSON.parse(contentStr);

        const success = await insertBusAccess(content);

        if (success) {
          console.log(
            "Mensaje procesado e insertado correctamente. Topic:",
            topic
          );
          await redisClient.del(messageId);
        } else {
          throw new Error("La inserción en BD falló");
        }
      } catch (err) {
        console.error("Error procesando el mensaje:", err.message);

        const retryCountStr = await redisClient.get(messageId);
        const currentRetryCount = retryCountStr
          ? parseInt(retryCountStr, 10)
          : 0;

        if (currentRetryCount < MAX_RETRIES) {
          const nextRetryDelay = (currentRetryCount + 1) * 10;

          console.log(
            `Reintentando mensaje en ${nextRetryDelay}s (intento ${
              currentRetryCount + 1
            }/${MAX_RETRIES}). ID: ${messageId}`
          );

          await redisClient.set(
            messageId,
            currentRetryCount + 1,
            "EX",
            nextRetryDelay
          );

          setTimeout(async () => {
            try {
              const producer = kafka.producer();
              await producer.connect();
              await producer.send({
                topic,
                messages: [{ value: message.value }],
              });
              await producer.disconnect();
              console.log(`Mensaje re-enviado a Kafka (ID: ${messageId})`);
            } catch (producerErr) {
              console.error(
                "Error al republicar el mensaje a Kafka:",
                producerErr.message
              );
            }
          }, nextRetryDelay * 1000);
        } else {
          console.warn(
            `Mensaje alcanzó el máximo de reintentos (${MAX_RETRIES}). Guardamos error en archivo. ID: ${messageId}`
          );

          let rawContent;
          try {
            rawContent = JSON.parse(message.value.toString());
          } catch (_) {
            rawContent = { rawPayload: message.value.toString() };
          }
          await saveErrorToFile(rawContent, topic, err);

          await redisClient.del(messageId);
        }
      }
    },
  });
}

runKafkaConsumer().catch((err) => {
  console.error("Error al iniciar el consumidor de Kafka:", err);
  process.exit(1);
});
