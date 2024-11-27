const express = require("express");
const app = express();
const { Kafka } = require("kafkajs");

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Kafka client setup
const kafka = new Kafka({
  clientId: "kafka-producer",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();

const initKafkaProducer = async () => {
  try {
    await producer.connect();
    console.log("Kafka producer connected");
  } catch (error) {
    console.error("Error connecting Kafka producer:", error);
    process.exit(1);
  }
};

app.post("/send", async function (req, res) {
  try {
    await producer.send({
      topic: "order",
      messages: [{ value: "success" }], // Corrected the key from 'message' to 'value'
    });
    res.status(200).send({ message: "Message sent to Kafka" });
  } catch (error) {
    console.error("Error sending message to Kafka:", error);
    res.status(500).send({ message: "Failed to send message" });
  }
});

app.listen(8000, async () => {
  await initKafkaProducer();
  console.log("Server is running on port 8000");
});
