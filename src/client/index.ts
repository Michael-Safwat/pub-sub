import amqp from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Successfully connected to RabbitMQ")
  
  const username = await clientWelcome();
  
  const exchange = ExchangePerilDirect;
  const queueName = `${PauseKey}.${username}`;
  const routingKey = PauseKey;
  const queueType = SimpleQueueType.Transient;

  const [channel, queue] = await declareAndBind(conn, exchange, queueName, routingKey, queueType);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
