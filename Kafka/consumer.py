from kafka import KafkaConsumer
import json
import time
producer = KafkaConsumer( bootstrap_servers='localhost:9092', maxbock_ms =5000)   

producer.send('users_create', b'Hello, Kafka!')