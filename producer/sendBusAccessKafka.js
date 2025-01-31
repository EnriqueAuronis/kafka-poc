// controllers/sendBusAccessKafka.js
require("dotenv").config();
const { Kafka } = require("kafkajs");
const { getProducer } = require("../config/kafka");

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || "my-kafka-client",
  brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
  connectionTimeout: 3000,
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

let producer;

async function getKafkaProducer() {
  if (!producer) {
    producer = kafka.producer();
    await producer.connect();
  }
  return producer;
}

const sendAccess = async (req, res) => {
  try {
    const debugQueue = process.env.QUEUE_DEBUG || 0;

    const no_tra = req.headers.no_traceability ?? "false";

    const data = req.body;
    if (!Array.isArray(data)) {
      return res.status(400).json({
        message: "Request body must be an array",
      });
    }

    if (debugQueue === "1") {
      console.log("Request Body:", data);
      console.log("Request Headers:", req.headers);
    }

    const producer = getProducer();

    for (let i = 0; i < data.length; i++) {
      const messageObject = {
        accessCounter: data[i].accessCounter,
        accessDate: data[i].accessDate,
        balanceNew: data[i].balanceNew,
        balanceOld: data[i].balanceOld,
        creationDate: data[i].creationDate,
        deviceId: data[i].deviceId,
        discountAmount: data[i].discountAmount,
        discountConcept: data[i].discountConcept,
        latitude: data[i].latitude,
        longitude: data[i].longitude,
        passengers: data[i].passengers,
        paymentCompany: data[i].paymentCompany,
        paymentCompanyId: data[i].paymentCompanyId,
        paymentMethod: data[i].paymentMethod,
        paymentMethodId: data[i].paymentMethodId,
        qrVersion: data[i].qrVersion,
        subtotal: data[i].subtotal,
        ticketId: data[i].ticketId,
        total: data[i].total,
        userCategory: data[i].userCategory,
        userId: data[i].userId,
        userProfile: data[i].userProfile,
        userTitle: data[i].userTitle,
        no_bank: data[i].no_bank,
        bank: data[i].bank,
        no_bank_flag: data[i].no_bank_flag,
        subscription: data[i].subscription,
      };

      let topic;
      if (no_tra === "true" && !messageObject.no_bank_flag) {
        topic = process.env.TOPIC_DACCESS_NO_TRACE || "dAccessNoTraceability";
      } else {
        topic = process.env.TOPIC_DACCESS || "dAccess";
      }

      if (debugQueue === "1") {
        console.log(`Sending to topic: ${topic}`);
        console.log("Message:", messageObject);
      }

      await producer.send({
        topic,
        messages: [{ value: JSON.stringify(messageObject) }],
      });
    }

    return res.status(200).json({
      message: "Datos enviados",
      length: data.length,
    });
  } catch (err) {
    console.error("Error sending data to Kafka:", err);
    return res.status(500).json({
      message: "Error al enviar los datos",
      error: err.message,
    });
  }
};

module.exports = { sendAccess };
