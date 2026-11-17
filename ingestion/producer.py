import os 
import json 
from datetime import datetime, timezone

from confluent_kafka import Producer

from dotenv import load_dotenv

load_dotenv()

config ={
     "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
    "client.id": "marketpulse-python-producer"
}

#keys check

bootstrap = os.getenv("BOOTSTRAP_SERVERS")
api_key = os.getenv("KAFKA_API_KEY")
api_secret = os.getenv("KAFKA_API_SECRET")
print("Bootstrap Server:", bool(bootstrap))
print("API Key Loaded:", bool(api_key))
print("API Secret Loaded:", bool(api_secret))


#initialze producer

producer=Producer(config)

#delivery callback

def delivery_report(err,msg):
    if err is not None:
        print(f"Delivate failed:{err}")

    else:
        print(
            f"Message successfully delivered |"
            f"Topic:{msg.topic()} |"
            f"Partition:{msg.partition()} |"
            f"Offset:{msg.offset()}"
        )

#sample trade event

trade={
    "trade_id":"trade_001",
    "produce_id":"BTC_USD",
    "price":"950000.50",
    "quantity":"0.025",
    "side":"BUY",
    "trade_time":datetime.now(timezone.utc).isoformat()
}

#publish message

# Publish message
producer.produce(
    topic="trades.raw",
    key=trade["produce_id"],
    value=json.dumps(trade),
    callback=delivery_report
)

# Wait for delivery
remaining = producer.flush(timeout=10)

if remaining == 0:
    print("All messages delivered successfully.")
else:
    print(f"WARNING: {remaining} messages were not delivered.")