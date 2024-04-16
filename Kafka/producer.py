import uuid
from datetime import datetime

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers='localhost:9092',)
    curr_time = time.time()
    producer.send('users_created', json.dumps('Helu kafka').encode('utf-8'))

stream_data()        