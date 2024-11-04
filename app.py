from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
from cryptography.fernet import Fernet, InvalidToken
import os
import logging
import json

conf = {
    'bootstrap.servers': '192.168.50.139:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}

logging.basicConfig(filename='consumer.log', level=logging.INFO,
                    format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
consumer = Consumer(conf)
consumer.subscribe(['quickstart-events'])
decryption_key = os.getenv('ENCRYPTION_KEY')
if decryption_key is None:
    raise ValueError("No decryption key found in environment variables")

cipher_suite = Fernet(decryption_key.encode())
messages_written = 0
with open('output.txt', 'a') as file:
    try:
        while True:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            try:
                # Log the raw message for debugging
                logging.info(f"Raw message: {msg.value()}")
                
                # Decrypt the message
                decrypted_message = cipher_suite.decrypt(msg.value())
                
                # Write the decrypted message to the file
                file.write(decrypted_message.decode('utf-8') + '\n')
                file.flush()
                messages_written += 1
                logging.info(f"Decrypted message: {decrypted_message.decode('utf-8')}")
                
                # Log the status of the service
                partitions = [TopicPartition(msg.topic(), msg.partition())]
                committed_offsets = consumer.committed(partitions)
                latest_offsets = consumer.position(partitions)
                queue_size = latest_offsets[0].offset - committed_offsets[0].offset
                logging.info(f"Messages written: {messages_written}, Messages remaining in queue: {queue_size}")
            except InvalidToken:
                logging.error("Invalid decryption key or corrupted message.")
    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer
        consumer.close()