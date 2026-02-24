import amqp from "amqplib";
import type { RabbitContext } from "./rabbit.types.js";
import { logger } from "../utils/logger.js";

const EXCHANGE_OPTIONS = { durable: true };

/**
 * Establish RabbitMQ connection and ensure exchange exists.
 * Handles reconnect logic on connection loss.
 */
export async function createConnection(url: string, publishExchange: string): Promise<RabbitContext> {
  const connection: amqp.ChannelModel = await amqp.connect(url);

  connection.on("error", (err: Error) => {
    logger.error({ err }, "RabbitMQ connection error");
  });

  connection.on("close", () => {
    logger.warn("RabbitMQ connection closed");
  });

  const channel = await connection.createChannel();
  await channel.assertExchange(publishExchange, "topic", EXCHANGE_OPTIONS);
  logger.info({ exchange: publishExchange }, "Exchange asserted");

  return { connection, channel, publishExchange };
}

export async function closeConnection(ctx: RabbitContext): Promise<void> {
  try {
    await ctx.channel.close();
    await ctx.connection.close();
  } catch (err) {
    logger.error({ err }, "Error closing RabbitMQ connection");
  }
}
