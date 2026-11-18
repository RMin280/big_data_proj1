from confluent_kafka import Producer
import socket
from dotenv import load_dotenv
import os
import finnhub
import time
import json

load_dotenv()

finnhub_key = os.getenv("FINNHUB_API_KEY")
kafka_topic = 'topic1'

conf = {'bootstrap.servers': 'kafka:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

finnhub_client = finnhub.Client(api_key=finnhub_key)

while True:
    msg = finnhub_client.general_news('general', min_id=0)
    producer.produce(kafka_topic, json.dumps(msg))
    print("messsage sent")

    time.sleep(1.0)