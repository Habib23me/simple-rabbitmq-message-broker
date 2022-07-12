import { JsonMessageBroker } from '../json_message_broker';
test('Send with reply works', async () => {
  try {
    const broker = await JsonMessageBroker.getInstance();
    const dataToSend = { data: 'hello' };
    const dataToReply = { data: 'hey' };

    const queue = 'test';
    broker.subscribe(queue, () => dataToReply);
    const data = await broker.sendWithReply(queue, dataToSend);
    console.log('Got data');

    console.log(data);
    expect(data).toEqual(dataToReply);
  } catch (e) {
    console.log(e);
  }
}, 1200000);
