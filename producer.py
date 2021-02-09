from kafka import KafkaProducer

KAFKA_SERVER="35.233.3.112:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER
)

print("Connexion :", producer.bootstrap_connected())

while True:
    message = input("Message : ")
    future=producer.send('test-kafka',str.encode(message))
    result = future.get(timeout=5)
    print(result)