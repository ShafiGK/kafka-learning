import json
import logging
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'learn-topic'
CONSUMER_GROUP = 'python-consumer-group'


def safe_json_deserializer(v):
    """
    Safely decode and parse JSON messages.
    Returns None if the message is malformed, preventing consumer crashes.
    """
    if v is None:
        return None
    try:
        return json.loads(v.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.warning(f"Malformed message skipped. Error: {e}. Raw data: {v}")
        return None


def create_consumer():
    """Create a Kafka consumer with manual commit."""
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset='earliest',
        enable_auto_commit=False,  # Manual commit for safety
        value_deserializer=safe_json_deserializer,
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )


def process_message(message):
    """Process a single message - implement business logic here."""
    if message.value is None:
        return False

    logger.info(
        f'Received: key={message.key}, value={message.value}, '
        f'partition={message.partition}, offset={message.offset}'
    )
    return True


def main():
    """Main consumer loop with manual offset commit."""
    consumer = create_consumer()
    logger.info(f'Consumer started for topic: {TOPIC_NAME}')

    try:
        for message in consumer:
            try:
                success = process_message(message)
                
                # Standard parameterless commit handles synchronous 
                # sequence offsetting perfectly across all library versions
                consumer.commit()
                
                if not success:
                    logger.info(f"Skipping invalid message at offset {message.offset}")

            except Exception as e:
                logger.error(f'Error processing message: {e}')
                
    except KeyboardInterrupt:
        logger.info('Stopping consumer...')
    finally:
        consumer.close()
        logger.info('Consumer closed cleanly')


if __name__ == '__main__':
    main()

