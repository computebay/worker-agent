import amqp from "amqplib";
import type { RabbitContext } from "./rabbit.types.js";
import { logger } from "../utils/logger.js";

/**
 * Establish RabbitMQ connection using provided credentials.
 * The scheduler is responsible for declaring exchanges/queues.
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
