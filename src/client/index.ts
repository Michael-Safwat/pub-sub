import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import { handlerPause } from "./handlers/handlerPause.js";
import { handlerMove } from "./handlers/handlerMove.js";
import { publishJSON } from "../internal/pubsub/publish.js";

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  const channel = await conn.createConfirmChannel();
  console.log("Successfully connected to RabbitMQ")

  const username = await clientWelcome();

  const exchange = ExchangePerilDirect;
  const queueName = `${PauseKey}.${username}`;
  const routingKey = PauseKey;
  const queueType = SimpleQueueType.Transient;

  //army move mq configuration
  const perilTopicExchange = ExchangePerilTopic;
  const armyMovesQueue = `${ArmyMovesPrefix}.${username}`;
  const armyMovesRoutingKey = `${ArmyMovesPrefix}.*`;


  await declareAndBind(conn, exchange, queueName, routingKey, queueType);
  await declareAndBind(conn, perilTopicExchange, armyMovesQueue, armyMovesRoutingKey, SimpleQueueType.Transient);

  const gameState: GameState = new GameState(username);
  subscribeJSON(conn, exchange, queueName, routingKey, queueType, handlerPause(gameState));
  subscribeJSON(conn, perilTopicExchange, armyMovesQueue, armyMovesRoutingKey, SimpleQueueType.Transient, handlerMove(gameState));

  while (true) {
    const input = await getInput();
    if (!input.length)
      continue;

    if (input[0] === "spawn") {
      runFunction(gameState, input, commandSpawn)
    } else if (input[0] === "move") {
      try {
        const armyMove: ArmyMove = runFunction(gameState, input, commandMove);
        publishJSON(channel, perilTopicExchange, armyMovesRoutingKey, armyMove);
        console.log(`Move message published successfully!`);
      } catch (e) {
        console.log(`an error occured while sending rabbitMq message: ${e}`)
      }

    } else if (input[0] === "status") {
      await commandStatus(gameState);
    } else if (input[0] === "help") {
      printClientHelp();
    } else if (input[0] === "spam") {
      console.log("Spamming not allowed yet!");
    } else if (input[0] === "quit") {
      printQuit()
      process.exit(0);
    } else {
      console.log("unknown command!");
    }
  }
}

function runFunction(gameState: GameState, input: string[], operation: (gameState: GameState, input: string[]) => any) {
  try {
    return operation(gameState, input);
  } catch (e) {
    if (e instanceof Error)
      console.log(e.message);
    else
      console.log(`an Error is encountered ${e}`);
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});