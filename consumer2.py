# consumer
import time
import json

from kafka import KafkaConsumer

BOOTSTRAP_SERVER = "35.233.3.112:9092"

consumer = KafkaConsumer(
    'exo_aleatoire',
    group_id='regular',
    bootstrap_servers=[BOOTSTRAP_SERVER],
    value_deserializer=lambda m: json.loads(m.decode('ascii')))

values = []
for msg in consumer:
    values.append(msg.value)
    if len(values) >= 4:
        values = values[1:]
    print("Somme cumulée : {}".format(sum(values)))