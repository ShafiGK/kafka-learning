from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
 bootstrap_servers=['localhost:9092'],
 value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(10):
 msg = {'id': i, 'message': f'Hello Kafka {i}', 'timestamp': time.time()}
 producer.send('learn-topic', value=msg)
 print(f'Sent: {msg}')
 time.sleep(1)

producer.flush()

producer.close()
