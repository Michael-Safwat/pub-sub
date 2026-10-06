import type { GameState, PlayingState } from "../../internal/gamelogic/gamestate.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => void{
    return (ps) => {
        if(ps.isPaused)
            gs.pauseGame()
        else
            gs.resumeGame()
        console.log("> ");
    }
}