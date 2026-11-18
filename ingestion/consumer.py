import os
from pathlib import Path
from dotenv import load_dotenv

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.error import SerializationError


# Load environment variables
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# (Optional) Load local schema for reader-side validation
SCHEMA_PATH = BASE_DIR / "schemas" / "trade.avsc"
with open(SCHEMA_PATH, "r") as file:
    TRADE_SCHEMA = file.read()

# Kafka Consumer Configuration
consumer_config = {
    "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
    "group.id": "marketpulse-avro-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}

# Schema Registry Configuration with fallback for env variable names
sr_api_key = os.getenv("SCHEMA_REGISTRY_API_KEY") or os.getenv("SCHEMA_REGISTRY_API")
sr_api_secret = os.getenv("SCHEMA_REGISTRY_API_SECRET") or os.getenv("SCHEMA_REGISTRY_SECRET")

schema_registry_config = {
    "url": os.getenv("SCHEMA_REGISTRY_URL"),
    "basic.auth.user.info": f"{sr_api_key}:{sr_api_secret}"
}


def main():
    consumer = Consumer(consumer_config)
    schema_registry_client = SchemaRegistryClient(schema_registry_config)
    
    # You can pass TRADE_SCHEMA or leave it None for dynamic schema resolution
    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        schema_str=TRADE_SCHEMA
    )

    consumer.subscribe(["trades.raw"])
    print("Listening for Avro messages on 'trades.raw'...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            if msg.value() is None:
                continue

            # Safely attempt Avro deserialization
            try:
                trade = avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE)
                )
            except SerializationError as se:
                print(f"[Warning] Skipped non-Avro message at Partition {msg.partition()}, Offset {msg.offset()}: {se}")
                consumer.commit(message=msg, asynchronous=False)
                continue

            print("\nReceived Trade:")
            print(trade)
            print(f"Partition: {msg.partition()} | Offset: {msg.offset()}")

            # Commit after successful processing
            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()