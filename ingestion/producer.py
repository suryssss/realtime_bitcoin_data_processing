import os 
import json 
from datetime import datetime, timezone

from pathlib import Path
from confluent_kafka import Producer

from dotenv import load_dotenv

BASE_DIR=Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR/ ".env")

#keys check

if __name__ == "__main__":
    bootstrap = os.getenv("BOOTSTRAP_SERVERS")
    api_key = os.getenv("KAFKA_API_KEY")
    api_secret = os.getenv("KAFKA_API_SECRET")
    print("Bootstrap Server:", bool(bootstrap))
    print("API Key Loaded:", bool(api_key))
    print("API Secret Loaded:", bool(api_secret))

#delivery callback

def delivery_report(err,msg):
    if err is not None:
        print(f"Delivery failed:{err}")

    else:
        print(
            f"Message successfully delivered |"
            f"Topic:{msg.topic()} |"
            f"Partition:{msg.partition()} |"
            f"Offset:{msg.offset()}"
        )
#creating a producer

def create_producer():
    config ={
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": os.getenv("KAFKA_API_KEY"),
        "sasl.password": os.getenv("KAFKA_API_SECRET"),
        "client.id": "marketpulse-python-producer"
        }

    return Producer(config)

def publish_trade(producer,trade):
    producer.produce(
        topic="trades.raw",
        key=trade["product_id"],
        value=json.dumps(trade),
        callback=delivery_report
    )

    #serve delivery callbacks
    producer.poll(0)