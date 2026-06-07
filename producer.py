#!/usr/bin/env python3
"""
Kafka Producer - Production-grade example
Author: Shafique Khan
"""

import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'learn-topic'

def create_producer():
    """Create a Kafka producer with reliability settings."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',          # Wait for all replicas
        retries=3,           # Retry on transient failures
        max_in_flight_requests_per_connection=1
    )

def send_message(producer, topic, key, value):
    """Send a single message with error handling."""
    try:
        future = producer.send(topic, key=key, value=value)
        metadata = future.get(timeout=10)
        logger.info(
            f'Sent message: key={key}, partition={metadata.partition}, '
            f'offset={metadata.offset}'
        )
    except KafkaError as e:
        logger.error(f'Failed to send message: {e}')

def main():
    """Main entry point."""
    producer = create_producer()
    
    try:
        for i in range(10):
            message = {
                'id': i,
                'message': f'Hello Kafka.This is Shafique!. My message number {i}',
                'timestamp': time.time()
            }
            send_message(producer, TOPIC_NAME, f'key-{i}', message)
            time.sleep(1)
    finally:
        producer.flush()
        producer.close()
        logger.info('Producer closed cleanly')

if __name__ == '__main__':
    main()
