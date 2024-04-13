from kafka import KafkaConsumer
topic_name = 'items'
consumer = KafkaConsumer(topic_name)
for message in consumer:
    print(message.value.decode('utf-8'))