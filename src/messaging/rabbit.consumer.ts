import type { Channel } from "amqplib";
import { logger } from "../utils/logger.js";

export type MessageHandler = (payload: unknown) => Promise<void>;

/**
 * Consume from a specific queue with manual ACK.
 * Prefetch = maxConcurrency
 * Returns consumerTag for cancellation.
 * Assumes the scheduler has already declared the queue.
 */
export async function consume(
  channel: Channel,
  queue: string,
  prefetch: number,
  handler: MessageHandler
): Promise<string> {
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
        const isInvalidPayload = err instanceof Error && err.name === "InvalidJobPayloadError";
        if (isInvalidPayload) {
          channel.nack(msg, false, false);
        } else {
          logger.error({ err, queue }, "Job handler threw, nack with requeue");
          channel.nack(msg, false, true);
        }
      }
    },
    { noAck: false }
  );

  logger.info({ queue, prefetch }, "Consumer started");
  return consumerTag;
}

export async function cancelConsumer(channel: Channel, consumerTag: string): Promise<void> {
  try {
    await channel.cancel(consumerTag);
  } catch {
    // Consumer may already be gone
  }
}
