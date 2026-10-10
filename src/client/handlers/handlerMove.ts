import type { ConfirmChannel } from "amqplib";
import type { ArmyMove, RecognitionOfWar } from "../../internal/gamelogic/gamedata.js";
import type { GameState } from "../../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../../internal/gamelogic/move.js";
import { AckType } from "../../internal/pubsub/consume.js";
import { publishJSON } from "../../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../../internal/routing/routing.js";

export function handlerMove(gameState: GameState, channel: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {
    return async (move: ArmyMove): Promise<AckType> => {
        const moveOutcome = handleMove(gameState, move);
        console.log(`Moved ${move.units.length} units to ${move.toLocation}`);
        process.stdout.write("> ");

        if (moveOutcome === MoveOutcome.MakeWar) {
            const routingKey = `${WarRecognitionsPrefix}.${gameState.getUsername()}`;
            const rw: RecognitionOfWar = {
                attacker: move.player,
                defender: gameState.getPlayerSnap(),
            };
            try {
                publishJSON(channel, ExchangePerilTopic, routingKey, rw)
                return AckType.Ack;
            } catch (e) {
                console.log("Error while publishing war message!");
                return AckType.NackRequeue;
            }
        }

        else if (moveOutcome === MoveOutcome.Safe)
            return AckType.Ack;
        else
            return AckType.NackDiscard;
    }
}