import type { RecognitionOfWar } from "../../internal/gamelogic/gamedata.js";
import { GameState } from "../../internal/gamelogic/gamestate.js";
import { handleWar, WarOutcome } from "../../internal/gamelogic/war.js";
import { AckType } from "../../internal/pubsub/consume.js";

export function handlerWar(gameState: GameState): (msg: RecognitionOfWar) => Promise<AckType> {

    return async (msg: RecognitionOfWar): Promise<AckType> => {
        const warResolution = handleWar(gameState, msg);
        const warResult = warResolution.result;
        let result = null;

        if (warResult === WarOutcome.NotInvolved)
            result = AckType.NackRequeue;
        else if (warResult === WarOutcome.NoUnits)
            result = AckType.NackDiscard;
        else if (warResult === WarOutcome.OpponentWon || warResult === WarOutcome.YouWon || warResult === WarOutcome.Draw)
            result = AckType.Ack;
        else {
            console.log("Error processing war result!");
            result = AckType.NackDiscard;
        }
        process.stdout.write("> ");
        return result;
    }
}