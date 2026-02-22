import type { Channel } from "amqplib";
import { EXCHANGE } from "./rabbit.types.js";
import { logger } from "../utils/logger.js";
import { getNodeId, signPayload } from "../security/identity.js";

const PUBLISH_OPTIONS = { persistent: true };

/**
 * Publish payload to exchange. Signs payload and adds nodeId, timestamp, signature.
 * Every outgoing message must be signed.
 */
export function publishSigned(
  channel: Channel,
  routingKey: string,
  payload: Record<string, unknown>
): void {
  const payloadWithMeta = {
    ...payload,
    nodeId: getNodeId(),
    timestamp: (payload.timestamp as string) ?? new Date().toISOString(),
  };
  const signature = signPayload(payloadWithMeta);
  const signed = { ...payloadWithMeta, signature };
  const body = Buffer.from(JSON.stringify(signed), "utf-8");
  const ok = channel.publish(EXCHANGE, routingKey, body, PUBLISH_OPTIONS);

  if (!ok) {
    logger.warn({ routingKey }, "Channel buffer full, message not published");
  }
}

