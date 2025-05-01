from kafka import KafkaConsumer
import json
import time
from kafka.errors import NoBrokersAvailable

print('start')

def setup_consumer():
    try:
        consumer = KafkaConsumer(
            'Topic1',
            bootstrap_servers=['broker1:9092', 'broker2:9093'],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='group1'
        )
        return consumer
    except NoBrokersAvailable:
        print('waiting for brokers to become available')
        return 'not-ready'
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 'not-ready'

print('starting consumer, checking if brokers are available')
consumer = 'not-ready'

while consumer == 'not-ready':
    print('brokers not available yet')
    time.sleep(5)
    consumer = setup_consumer()

print('brokers are available and ready to consume messages')

for message in consumer:
    print(f"Topic1: {message.value}")
