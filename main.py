import json
import asyncio
from logger import logger
import pulsar
from config import PULSAR_URL, TOKEN, TOPIC, SUBSCRIPTION_NAME
from message_processor import MessageProcessor
# adding test comment
class PulsarClient:
    def __init__(self, url, token):
        self.client = pulsar.Client(
            url,
            tls_trust_certs_file_path='./ca-chain.crt',
            authentication=pulsar.AuthenticationToken(token) if token else None
        )
        self.consumer = self.client.subscribe(
            TOPIC,
            SUBSCRIPTION_NAME,
            consumer_type=pulsar.ConsumerType.Shared
        )
    def close(self):
        self.client.close()

def dump_message(message):
    logger.info(f"Received message: {message.data().decode('utf-8')}")
    # Dump the message to a file or any other storage
    with open('messages_dump.txt', 'a') as file:
        file.write(message.data().decode('utf-8') + '\n')

async def consume_messages(consumer, processor):
    loop = asyncio.get_event_loop()
    while True:
        try:
            msg = await loop.run_in_executor(None, consumer.receive)
            #dump_message(msg)
            await processor.process_message(msg)
            consumer.acknowledge(msg)
        except Exception as e:
            logger.error(f"consume_messages: Failed to receive or process message: {e}")
def main():
    pulsar_client = PulsarClient(PULSAR_URL, TOKEN)
    processor = MessageProcessor()
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(consume_messages(pulsar_client.consumer, processor))
    finally:
        pulsar_client.close()
        loop.close()
if __name__ == '__main__':
    main()




