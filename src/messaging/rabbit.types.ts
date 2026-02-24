import type { Channel, ChannelModel } from "amqplib";

export interface RabbitContext {
  connection: ChannelModel;
  channel: Channel;
  publishExchange: string;
}
