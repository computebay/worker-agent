import type { Channel, ChannelModel } from "amqplib";

export const EXCHANGE = "compute-bay.jobs";

export interface RabbitContext {
  connection: ChannelModel;
  channel: Channel;
}
