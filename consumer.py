from kafka import KafkaConsumer
import json

def safe_json_deserializer(v):
    if v is None or v == b'':
        return None
    try:
        return json.loads(v.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError):
        # Return the raw bytes/string if not valid JSON
        return v.decode('utf-8', errors='replace')

consumer = KafkaConsumer(
    'learn-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='python-consumer-group',
    value_deserializer=safe_json_deserializer
)

print('Listening for messages...')
for message in consumer:
    value = message.value
    print(f'Received: {value} (partition: {message.partition}, offset: {message.offset})')
