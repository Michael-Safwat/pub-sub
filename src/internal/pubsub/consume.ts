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

  const queueArguments: Record<string, any> = {
    'x-dead-letter-exchange': 'peril_dlx',
  };

  const queue = await channel.assertQueue(queueName,
    {
      durable: durable,
      autoDelete: autoDelete,
      exclusive: exclusive,
      arguments: { "x-dead-letter-exchange": "peril_dlx", }
    });

  channel.bindQueue(queue.queue, exchange, key);
  return [channel, queue]
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  const [channel, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);
  await channel.consume(queue.queue, async (msg: amqp.ConsumeMessage | null) => {
    if (!msg)
      return
    const msgContent = JSON.parse(String(msg.content));
    console.log("received a message");
    const ackType = await handler(msgContent);

    if (ackType === AckType.Ack) {
      channel.ack(msg);
      console.log("Ack message!");
    }
    else if (ackType === AckType.NackDiscard) {
      channel.nack(msg, false, false);
      console.log("Nack discard message!");
    }
    else {
      channel.nack(msg, false, true);
      console.log("Nack requeue message!");
    }
  });
}

export enum SimpleQueueType {
  Durable,
  Transient,
}

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard
}