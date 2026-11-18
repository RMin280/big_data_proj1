from confluent_kafka import Producer
import socket
from dotenv import load_dotenv
import os
import finnhub
import time
import json

load_dotenv()

def main():
    finnhub_key = os.getenv("FINNHUB_API_KEY")
    kafka_topic = 'topic1'

    conf = {'bootstrap.servers': 'kafka:9092',
            'client.id': socket.gethostname()}

    producer = Producer(conf)

    finnhub_client = finnhub.Client(api_key=finnhub_key)

    last_id = 0

    while True:
        msg = finnhub_client.general_news('general', min_id=last_id)
        print(msg)
        producer.produce(kafka_topic, json.dumps(msg))
        for it in sorted(msg, key=lambda x: x["id"]):
            raw = json.dumps({"id": it["id"], "datetime": it["datetime"], "headline": it["headline"], "summary": it["summary"]})
            producer.produce("topic1", raw)
            last_id = max(last_id, it["id"])
        producer.flush()
        time.sleep(1)


if __name__ == "__main__":
    main()