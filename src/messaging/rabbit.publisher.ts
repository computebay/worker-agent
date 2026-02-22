import type { Channel } from "amqplib";
import { EXCHANGE } from "./rabbit.types.js";
import { logger } from "../utils/logger.js";
import { signPayload } from "../security/identity.js";

const PUBLISH_OPTIONS = { persistent: true };

export function publish(channel: Channel, routingKey: string, payload: object): void {
  const body = Buffer.from(JSON.stringify(payload), "utf-8");
  const ok = channel.publish(EXCHANGE, routingKey, body, PUBLISH_OPTIONS);
  if (!ok) {
    logger.warn({ routingKey }, "Channel buffer full, message not published");
  }
}

/**
 * Publish heartbeat with signature. Scheduler verifies using stored public key.
 */
export function publishHeartbeat(
  channel: Channel,
  payload: Record<string, unknown>
): void {
  const payloadWithMeta = {
    ...payload,
    timestamp: (payload.timestamp as string) ?? new Date().toISOString(),
  };
  const signature = signPayload(payloadWithMeta);
  const signed = { ...payloadWithMeta, signature };
  const body = Buffer.from(JSON.stringify(signed), "utf-8");
  const ok = channel.publish(EXCHANGE, "node.heartbeat", body, PUBLISH_OPTIONS);
  if (!ok) {
    logger.warn("Channel buffer full, heartbeat not published");
  }
}

