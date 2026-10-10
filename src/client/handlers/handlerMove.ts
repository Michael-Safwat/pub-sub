import type { ArmyMove } from "../../internal/gamelogic/gamedata.js";
import type { GameState } from "../../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../../internal/gamelogic/move.js";
import { AckType } from "../../internal/pubsub/consume.js";

export function handlerMove(gameState: GameState): (move: ArmyMove) => AckType {
    return (move: ArmyMove): AckType => {
        const moveOutcome = handleMove(gameState, move);
        console.log(`Moved ${move.units.length} units to ${move.toLocation}`);
        process.stdout.write("> ");

        if(moveOutcome === MoveOutcome.MakeWar || moveOutcome === MoveOutcome.Safe)
            return AckType.Ack;
        else
            return AckType.NackDiscard;
    }
}