import type { ArmyMove } from "../../internal/gamelogic/gamedata.js";
import type { GameState } from "../../internal/gamelogic/gamestate.js";
import { handleMove } from "../../internal/gamelogic/move.js";

export function handlerMove(gameState: GameState): (move: ArmyMove) => void {
    return (move: ArmyMove) => {
        handleMove(gameState, move);
        console.log(`Moved ${move.units.length} units to ${move.toLocation}`);
        process.stdout.write("> ");
    }
}