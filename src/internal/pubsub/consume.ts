import amqp, { type Channel } from "amqplib";

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {

  const channel = await conn.createChannel();
  const durable = queueType === SimpleQueueType.Durable ? true : false;
  const autoDelete = queueType === SimpleQueueType.Transient ? true : false;
  const exclusive = queueType === SimpleQueueType.Transient ? true : false;

  const queue = await channel.assertQueue(queueName, { durable: durable, autoDelete: autoDelete, exclusive: exclusive });
  channel.bindQueue(queue.queue, exchange, key);
  return [channel, queue]
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => void,
): Promise<void> {
  const [channel, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);
  channel.consume(queue.queue, (msg: amqp.ConsumeMessage | null) => {
    if (!msg)
      return
    const msgContent = JSON.parse(String(msg.content));
    console.log("received a message");
    handler(msgContent);
    channel.ack(msg);
    
  });
}


export enum SimpleQueueType {
  Durable,
  Transient,
}