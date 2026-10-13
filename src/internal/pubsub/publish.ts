import { encode } from "@msgpack/msgpack";
import type { ConfirmChannel } from "amqplib"

export async function publishJSON<T>(ch: ConfirmChannel, exchange: string, routingKey: string, value: T): Promise<void> {

    const bufferedJson = Buffer.from(JSON.stringify(value))
    ch.publish(exchange, routingKey, bufferedJson, { contentType: "application/json" });
}

export function publishMsgPack<T>(ch: ConfirmChannel, exchange: string, routingKey: string, value: T): Promise<void> {
    const body = encode(value);
    return new Promise((resolve, reject) => {
        ch.publish(exchange, routingKey, Buffer.from(body), { contentType: "application/x-msgpack" },
            (err) => {
                if (err !== null) {
                    reject(new Error("Message was NACKed by the broker"));
                } else {
                    resolve();
                }
            },
        );
    });
}