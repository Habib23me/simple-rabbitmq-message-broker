import * as amqplib from 'amqplib';
import * as _ from 'lodash';
export declare type MessageHandler = (msg: any) => any;
import { v1 as uuidv1 } from 'uuid';

/**
 * Broker for async messaging
 */
export abstract class MessageBroker {
  queues: any;
  connection?: amqplib.Connection;
  _channel?: amqplib.Channel;

  get channel(): amqplib.Channel {
    if (this._channel) {
      return this._channel;
    }
    throw new Error('Channel not created');
  }
  /**
   * Trigger init connection method
   */
  constructor() {
    this.queues = {};
  }

  /**
   * Initialize connection to rabbitMQ
   */
  async init(): Promise<MessageBroker> {
    try {
      const path = process.env.RABBITMQ_URL || 'amqp://localhost';
      console.log('Connecting Rabbit MQ: ' + path);

      this.connection = await amqplib.connect(path);
      this._channel = await this.connection?.createChannel();
      console.log('Connected to Rabbit MQ ' + path);

      return this;
    } catch (e) {
      await new Promise((_) => setTimeout(_, 1000));
      console.log('Rabbit MQ Error: ' + e);
      return this.init();
    }
  }
  /**
  
    /**
     * Send message to queue
     * @param {String} queue Queue name
     * @param {Object} msg Message as Buffer
     */
  async send(queue: string, msg: any) {
    if (!this.connection) {
      await this.init();
    }
    await this.channel.assertQueue(queue, { durable: true });
    return this.channel.sendToQueue(queue, msg);
  }
  async sendWithReply(queue: string, msg: any) {
    const ch = this.channel;
    if (!this.connection) {
      await this.init();
    }
    await ch.assertQueue(queue, { durable: true });

    return new Promise(async function (resolve, reject) {
      var corrId = uuidv1();
      function maybeAnswer(msg: any) {
        if (msg.properties.correlationId === corrId) {
          resolve(msg.content.toString());
        }
      }
      var replyTo = (await ch.assertQueue('', { exclusive: true })).queue;
      await ch.consume(replyTo, maybeAnswer, { noAck: true });
      ch.sendToQueue(queue, msg, {
        correlationId: corrId,
        replyTo: queue,
      });
    });
  }

  /**
   * @param {String} queue Queue name
   * @param {Function} handler Handler that will be invoked with given message and acknowledge function (msg, ack)
   */
  async subscribe(queue: string, handler: MessageHandler) {
    if (!this.connection) {
      await this.init();
    }
    if (this.queues[queue]) {
      const existingHandler = _.find(this.queues[queue], (h: MessageHandler) => h === handler);
      if (existingHandler) {
        return () => this.unsubscribe(queue, existingHandler);
      }
      this.queues[queue].push(handler);
      return () => this.unsubscribe(queue, handler);
    }

    await this.channel.assertQueue(queue, { durable: true });

    this.queues[queue] = [handler];

    this.channel.prefetch(1);
    this.channel.consume(queue, async (msg: any) => {
      console.log(`[ ${new Date()} ] Message received: ${JSON.stringify(JSON.parse(msg.content.toString('utf8')))}`);
      if (msg !== null) {
        for (const handler of this.queues[queue]) {
          const data = await handler(msg, () => {});
          if (msg.properties.replyTo && data) {
            this.channel.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify(data)), {
              correlationId: msg.properties.correlationId,
            });
          }
        }
        this.channel.ack(msg);
      }
    });
    return () => this.unsubscribe(queue, handler);
  }

  async unsubscribe(queue: string, handler: MessageHandler) {
    _.pull(this.queues[queue], handler);
  }
}
