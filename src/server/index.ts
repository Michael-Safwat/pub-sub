import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { SimpleQueueType, subscribeMsgPack } from "../internal/pubsub/consume.js";
import { handlerLog } from "../client/handlers/handlerLog.js";

async function main() {

  process.on('SIGINT', () => {
    conn.close()
    console.log("Exiting program")
  })

  console.log("Starting Peril server...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Successfully connected to RabbitMQ")

  subscribeMsgPack(
    conn,
    ExchangePerilTopic,
    GameLogSlug,
    `${GameLogSlug}.*`,
    SimpleQueueType.Durable,
    handlerLog(),
  );

  printServerHelp();

  while (true) {
    const input = await getInput();
    if (!input.length)
      continue;

    if (input[0] === "pause") {
      console.log("Sending a pause message!");
      publishPauseMessage(conn, true);
    } else if (input[0] === "resume") {
      console.log("Sending a resume message!");
      publishPauseMessage(conn, false);
    } else if (input[0] === "quit") {
      console.log("Exiting program")
      break;
    } else {
      console.log("Unknown command!");
    }
  }
}

async function publishPauseMessage(conn: amqp.ChannelModel, isPaused: boolean) {
  const channel = await conn.createConfirmChannel();
  const exchange = ExchangePerilDirect;
  const routingKey = PauseKey;
  const gameState: PlayingState = { isPaused: isPaused };

  publishJSON(channel, exchange, routingKey, gameState);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
