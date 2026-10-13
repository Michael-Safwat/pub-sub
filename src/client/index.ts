import amqp, { type ConfirmChannel } from "amqplib";
import { clientWelcome, commandStatus, getInput, getMaliciousLog, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import { handlerPause } from "./handlers/handlerPause.js";
import { handlerMove } from "./handlers/handlerMove.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import { handlerWar } from "./handlers/handlerWar.js";
import type { GameLog } from "../internal/gamelogic/logs.js";

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

  //ware mq configuration
  const warQueueName = "war";
  const warRoutingKey = `${WarRecognitionsPrefix}.*`;
  const warQueueType = SimpleQueueType.Durable;

  const gameState: GameState = new GameState(username);

  subscribeJSON(conn, exchange, queueName, routingKey, queueType, handlerPause(gameState));
  subscribeJSON(conn, perilTopicExchange, armyMovesQueue, armyMovesRoutingKey, SimpleQueueType.Transient, handlerMove(gameState, channel));
  subscribeJSON(conn, perilTopicExchange, warQueueName, warRoutingKey, warQueueType, handlerWar(channel, gameState));

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
      if (input.length < 2) {
        console.log("usage: spam <number>")
        continue;
      }
      if(!input[1]) 
      {
        console.log("usage: spam <number>");
        continue;
      }
      const num = parseInt(input[1], 10)
      if (isNaN(num)) {
        console.log(`error: ${input[1]} is not a valid number`);
        continue;
      }
      for (let i = 0; i < num; i++) {
        await publishGameLog(channel, gameState.getUsername(),  getMaliciousLog());
      }
      console.log(`Published ${num} malicious logs`);
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

export async function publishGameLog(ch: ConfirmChannel, username: string, msg: string) {
  const currentTime: Date = new Date();
  const gameLog: GameLog = { username: username, message: msg, currentTime: currentTime };
  publishMsgPack(ch, ExchangePerilTopic, `${GameLogSlug}.${username}`, gameLog);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});