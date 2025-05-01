from kafka import KafkaProducer
import csv
import json
import time
from kafka.errors import NoBrokersAvailable

print('start')

def setup_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['broker1:9092', 'broker2:9093'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    except NoBrokersAvailable:
        print('waiting for brokers to become available')
        return 'not-ready'
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 'not-ready'

print('setting up producer, checking if brokers are available')
producer = 'not-ready'

while producer == 'not-ready':
    print('brokers not available yet')
    time.sleep(5)
    producer = setup_producer()

print('brokers are available and ready to produce messages')

try:
    with open('data.csv', mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            producer.send('Topic1', row)
            producer.send('Topic2', row)
            print(f"Sent: {row}")
            time.sleep(1)
except FileNotFoundError:
    print('data.csv not found inside the container!')

producer.flush()
