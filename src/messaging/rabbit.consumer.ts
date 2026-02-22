import type { Channel } from "amqplib";
import { EXCHANGE } from "./rabbit.types.js";
import { logger } from "../utils/logger.js";

const QUEUE_OPTIONS = { durable: true };

export type MessageHandler = (payload: unknown) => Promise<void>;

/**
 * Consume from worker queue with manual ACK.
 * Queue: worker.{nodeId}
 * Routing key: job.scheduled.{nodeId}
 * Prefetch = maxConcurrency
 * Returns consumerTag for cancellation.
 */
export async function consume(
  channel: Channel,
  nodeId: string,
  prefetch: number,
  handler: MessageHandler
): Promise<string> {
  const queue = `worker.${nodeId}`;
  const routingKey = `job.scheduled.${nodeId}`;

  await channel.assertQueue(queue, QUEUE_OPTIONS);
  await channel.bindQueue(queue, EXCHANGE, routingKey);
  await channel.prefetch(prefetch);

  const { consumerTag } = await channel.consume(
    queue,
    async (msg) => {
      if (!msg) return;

      try {
        const payload = JSON.parse(msg.content.toString("utf-8"));
        logger.debug({ payload }, "Message received");
        await handler(payload);
        channel.ack(msg);
      } catch (err) {
        logger.error({ err, queue }, "Job handler threw, nack with requeue");
        channel.nack(msg, false, true);
      }
    },
    { noAck: false }
  );

  logger.info({ queue, routingKey, prefetch }, "Consumer started");
  return consumerTag;
}

export async function cancelConsumer(channel: Channel, consumerTag: string): Promise<void> {
  try {
    await channel.cancel(consumerTag);
  } catch {
    // Consumer may already be gone
  }
}
