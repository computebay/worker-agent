import type { Channel, Connection } from "amqplib";

export const EXCHANGE = "compute-bay.jobs";

export interface RabbitContext {
  connection: Connection;
  channel: Channel;
}
