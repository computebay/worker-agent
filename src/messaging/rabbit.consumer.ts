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

export type CancelHandler = (jobId: string) => void;

/**
 * Consume job cancel requests from a queue.
 * Message format: { jobId: string }
 * Returns consumerTag for cancellation.
 */
export async function consumeCancel(
  channel: Channel,
  cancelQueue: string,
  onCancel: CancelHandler
): Promise<string> {
  await channel.assertQueue(cancelQueue, { durable: true });

  const { consumerTag } = await channel.consume(
    cancelQueue,
    (msg) => {
      if (!msg) return;
      try {
        const payload = JSON.parse(msg.content.toString("utf-8"));
        const jobId = payload?.jobId;
        if (typeof jobId === "string") {
          onCancel(jobId);
        }
        channel.ack(msg);
      } catch {
        channel.nack(msg, false, false);
      }
    },
    { noAck: false }
  );

  logger.info({ cancelQueue }, "Cancel consumer started");
  return consumerTag;
}

/**
 * Priority cancel consumer on the dedicated `<queue>.cancel` lane.
 * No declare here: the queue is declared by privileged services (scheduler
 * at registration, job-service on emit) because the worker's scoped user
 * cannot configure anything. Only CANCEL payloads are ever published here,
 * so anything else is dropped (never requeued — that would hot-loop).
 */
export async function consumeCancelShared(
  channel: Channel,
  queue: string,
  onCancel: CancelHandler
): Promise<string> {
  const { consumerTag } = await channel.consume(
    queue,
    (msg) => {
      if (!msg) return;
      try {
        const payload = JSON.parse(msg.content.toString("utf-8"));
        const typeStr = String(payload?.type ?? payload?.jobType ?? "").toLowerCase();
        const jobId = payload?.jobId;
        if (typeof jobId === "string" && (typeStr === "cancel" || typeStr === "cancelled")) {
          onCancel(jobId);
        }
      } catch {
        // Unparseable — drop, never requeue into a hot loop.
      } finally {
        try {
          channel.ack(msg);
        } catch {
          // Channel gone — shutting down.
        }
      }
    },
    { noAck: false }
  );

  logger.info({ queue }, "Priority cancel consumer started");
  return consumerTag;
}
