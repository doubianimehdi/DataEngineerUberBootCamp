# producer
import time
import json
import random

from kafka import KafkaProducer

BOOTSTRAP_SERVER = "35.233.3.112:9092"

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVER],
    value_serializer=lambda m: json.dumps(m).encode('ascii')
)

def on_success(record):
    print("Message envoyé : topic '{}' - offset {}." \
        .format(record.topic, record.offset))

def on_error(excp):
    print('An error occured while sending message')

start_time = time.time()

while True:
    producer.send('exo_aleatoire', random.randint(0, 10)) \
        .add_callback(on_success) \
        .add_errback(on_error)
    time.sleep(1.0 - ((time.time() - start_time) % 1.0))