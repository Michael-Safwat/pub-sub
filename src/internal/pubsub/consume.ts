import amqp, { type Channel } from "amqplib";

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]>{

    const channel = await conn.createChannel();
    const durable = queueType === SimpleQueueType.Durable? true: false;
    const autoDelete = queueType === SimpleQueueType.Transient? true: false;
    const exclusive  = queueType === SimpleQueueType.Transient? true: false;

    const queue = await channel.assertQueue(queueName,{durable: durable, autoDelete: autoDelete, exclusive: exclusive});
    channel.bindQueue(queue.queue, exchange, key);
    return [channel,queue]
}

export enum SimpleQueueType {
  Durable,
  Transient,
}