from kafka import KafkaProducer
topic_name = 'items'
producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send(topic_name, b'Hello, Kafka!')
producer.flush()