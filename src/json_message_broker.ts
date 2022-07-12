import { Completer } from './completer';
import { MessageBroker, MessageHandler } from './message-broker';


export class JsonMessageBroker extends MessageBroker {
    static instance: JsonMessageBroker;
    static request: Completer<JsonMessageBroker>;

    static async getInstance(): Promise<JsonMessageBroker> {
        if (!JsonMessageBroker.instance) {
            const broker = new JsonMessageBroker();
            if (!this.request) {
                this.request = new Completer<JsonMessageBroker>();
                broker.init().then(async () => {
                    JsonMessageBroker.instance = broker;
                    this.request.complete(broker);
                });
            }

            return this.request.promise;
        }
        return JsonMessageBroker.instance;
    }

    serialize(data: any) {
        return Buffer.from(JSON.stringify(data));
    }

    deserialize(handler: MessageHandler) {
        return async (data: any) => {
            return handler(JSON.parse(data.content.toString()));
        };
    }

    async send(queue: string, msg: any) {
        return super.send(queue, this.serialize(msg));
    }
    async sendWithReply(queue: string, msg: any) {
        const response: any = await super.sendWithReply(
            queue,
            this.serialize(msg)
        );
        return this.deserialize(JSON.parse(response.content.toString()));
    }
    async subscribe(queue: string, handler: MessageHandler) {
        return super.subscribe(queue, this.deserialize(handler));
    }
}

export const jsonMessageBroker = new JsonMessageBroker();
