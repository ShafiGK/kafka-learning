#!/usr/bin/env python3
"""
Kafka Consumer - Production-grade with manual offset management
Author: Shafique Khan
"""

import json
import logging
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'learn-topic'
CONSUMER_GROUP = 'learn-consumer-group'

def create_consumer():
    """Create a Kafka consumer with manual commit."""
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset='earliest',
        enable_auto_commit=False,    # Manual commit for safety
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )

def process_message(message):
    """Process a single message - implement business logic here."""
    logger.info(
        f'Received: key={message.key}, value={message.value}, '
        f'partition={message.partition}, offset={message.offset}'
    )
    # Add your business logic here
    return True

def main():
    """Main consumer loop with manual offset commit."""
    consumer = create_consumer()
    logger.info(f'Consumer started for topic: {TOPIC_NAME}')
    
    try:
        for message in consumer:
            try:
                if process_message(message):
                    consumer.commit()
            except Exception as e:
                logger.error(f'Error processing message: {e}')
                # Don't commit - message will be redelivered
    except KeyboardInterrupt:
        logger.info('Stopping consumer...')
    finally:
        consumer.close()
        logger.info('Consumer closed cleanly')

if __name__ == '__main__':
    main()
