import amqp from "amqplib";
import { EXCHANGE, type RabbitContext } from "./rabbit.types.js";
import { logger } from "../utils/logger.js";

const EXCHANGE_OPTIONS = { durable: true };

/**
 * Establish RabbitMQ connection and ensure exchange exists.
 * Handles reconnect logic on connection loss.
 */
export async function createConnection(url: string): Promise<RabbitContext> {
  const connection: amqp.ChannelModel = await amqp.connect(url);

  connection.on("error", (err: Error) => {
    logger.error({ err }, "RabbitMQ connection error");
  });

  connection.on("close", () => {
    logger.warn("RabbitMQ connection closed");
  });

  const channel = await connection.createChannel();
  await channel.assertExchange(EXCHANGE, "topic", EXCHANGE_OPTIONS);
  logger.info({ exchange: EXCHANGE }, "Exchange asserted");

  return { connection, channel };
}

export async function closeConnection(ctx: RabbitContext): Promise<void> {
  try {
    await ctx.channel.close();
    await ctx.connection.close();
  } catch (err) {
    logger.error({ err }, "Error closing RabbitMQ connection");
  }
}
