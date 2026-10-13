import type { ConfirmChannel } from "amqplib";
import type { RecognitionOfWar } from "../../internal/gamelogic/gamedata.js";
import { GameState } from "../../internal/gamelogic/gamestate.js";
import { handleWar, WarOutcome } from "../../internal/gamelogic/war.js";
import { AckType } from "../../internal/pubsub/consume.js";
import { publishGameLog } from "../index.js";

export function handlerWar(channel: ConfirmChannel, gameState: GameState): (msg: RecognitionOfWar) => Promise<AckType> {

    return async (msg: RecognitionOfWar): Promise<AckType> => {
        const warResolution = handleWar(gameState, msg);
        const warResult = warResolution.result;
        let result = null;

        if (warResult === WarOutcome.NotInvolved)
            result = AckType.NackRequeue;
        else if (warResult === WarOutcome.NoUnits)
            result = AckType.NackDiscard;
        else if (warResult === WarOutcome.OpponentWon || warResult === WarOutcome.YouWon || warResult === WarOutcome.Draw) {
            let logMessage: string = "";
            if (warResult === WarOutcome.YouWon) {
                logMessage = `${warResolution.winner} won a war against ${warResolution.loser}`
            } else if (warResult === WarOutcome.OpponentWon) {
                logMessage = `${warResolution.winner} won a war against ${warResolution.loser}`
            } else if (warResult === WarOutcome.Draw) {
                logMessage = `A war between ${warResolution.attacker} and ${warResolution.defender} resulted in a draw`
            }

            try {
                publishGameLog(channel, gameState.getUsername(), logMessage);
                result = AckType.Ack;
            } catch (e) {
                result = AckType.NackRequeue;
            }
        }
        else {
            console.log("Error processing war result!");
            result = AckType.NackDiscard;
        }
        process.stdout.write("> ");
        return result;
    }
}