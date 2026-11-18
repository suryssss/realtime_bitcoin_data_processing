import os 
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer  # Fixed import
from confluent_kafka.serialization import SerializationContext, MessageField
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# Load Avro schema from schemas/ (plural)
SCHEMA_PATH = BASE_DIR / "schemas" / "trade.avsc"
with open(SCHEMA_PATH, "r") as file:
    TRADE_SCHEMA = file.read()


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Message successfully delivered | "
            f"Topic: {msg.topic()} | "
            f"Partition: {msg.partition()} | "
            f"Offset: {msg.offset()}"
        )


def create_producer():
    config = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": os.getenv("KAFKA_API_KEY"),
        "sasl.password": os.getenv("KAFKA_API_SECRET"),
        "client.id": "marketpulse-python-producer"
    }
    return Producer(config)


def create_avro_serializer():
    api_key = os.getenv("SCHEMA_REGISTRY_API_KEY") or os.getenv("SCHEMA_REGISTRY_API")
    api_secret = os.getenv("SCHEMA_REGISTRY_API_SECRET") or os.getenv("SCHEMA_REGISTRY_SECRET")

    schema_registry_config = {
        "url": os.getenv("SCHEMA_REGISTRY_URL"),
        "basic.auth.user.info": f"{api_key}:{api_secret}"  # Colon-separated format
    }

    schema_registry_client = SchemaRegistryClient(schema_registry_config)

    return AvroSerializer(
        schema_registry_client,
        TRADE_SCHEMA,
        conf={
            "auto.register.schemas": True
        }
    )


def publish_trade(producer, serializer, trade):
    # Serialize Python dictionary to Avro binary format
    serialized_value = serializer(
        trade,
        SerializationContext("trades.raw", MessageField.VALUE)
    )
    
    producer.produce(
        topic="trades.raw",
        key=str(trade["product_id"]),
        value=serialized_value,  # Pass the Avro serialized bytes
        callback=delivery_report
    )
    producer.poll(0)


if __name__ == "__main__":
    print("Bootstrap Server:", bool(os.getenv("BOOTSTRAP_SERVERS")))
    print("Kafka API Key:", bool(os.getenv("KAFKA_API_KEY")))
    print("Schema Registry URL:", bool(os.getenv("SCHEMA_REGISTRY_URL")))

    # Quick local test
    prod = create_producer()
    ser = create_avro_serializer()
    print("Producer and Avro Serializer initialized successfully!")