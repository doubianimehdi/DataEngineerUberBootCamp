from kafka import KafkaConsumer
from datetime import datetime

KAFKA_SERVER="35.233.3.112:9092"

consumer = KafkaConsumer(
    'test-kafka',
    bootstrap_servers=KAFKA_SERVER,
    group_id='regular'
)

for msg in consumer:
    date = datetime.fromtimestamp(int (msg.timestamp/1000))
    print("{} : {} (offset {})".format(
        date.strftime("%d/%m/%Y %H:%M:%S"),
        msg.value.decode("utf-8"),
        msg.offset    
    ))