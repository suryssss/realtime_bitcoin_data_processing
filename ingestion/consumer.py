import os 
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

# loading configurations
config ={
     "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
    "group.id": "marketpulse-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}

#initalzing consumer

consumer=Consumer(config)

#subscribing to the particular topic (trades.raw)
consumer.subscribe(["trades.raw"])

print("consumer_started waitig for messages")

#retreveing the messages from the kafka 
try:
    while True:
        msg=consumer.poll(1.0) #poll is used to retrive the message from the kafka 

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error:{msg.error()}")
            continue

        print("\nMessage received")
        print(f"Topic : {msg.topic()}")
        print(f"Partiton :{msg.partition()}")
        print(f"Offset : {msg.offset()}")
        print(f"Key : {msg.key()}")
        print(f"Value: {msg.value()}")

        consumer.commit(message=msg,asynchronous=False) #commits the processed message offset (saves consumers offset in kafka not the message )

except KeyboardInterrupt:
    print("\nConsumer Stopped")

finally:
    consumer.close()



