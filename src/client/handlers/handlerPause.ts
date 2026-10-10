import type { GameState, PlayingState } from "../../internal/gamelogic/gamestate.js";
import { AckType } from "../../internal/pubsub/consume.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType{
    return (ps): AckType => {
        if(ps.isPaused)
            gs.pauseGame()
        else
            gs.resumeGame()
        console.log("> ");

        return AckType.Ack
    }
}