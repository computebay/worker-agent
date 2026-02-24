import type { Channel } from "amqplib";
import { logger } from "../utils/logger.js";

const PUBLISH_OPTIONS = { persistent: true };

export function publish(
  channel: Channel,
  exchange: string,
  routingKey: string,
  payload: object
): void {
  const body = Buffer.from(JSON.stringify(payload), "utf-8");
  const ok = channel.publish(exchange, routingKey, body, PUBLISH_OPTIONS);
  if (!ok) {
    logger.warn({ exchange, routingKey }, "Channel buffer full, message not published");
  }
}
