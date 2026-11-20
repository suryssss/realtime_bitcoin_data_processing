from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(
    name="bronze.candles_1m",
    table_properties={"quality": "bronze"}
)
def candles_1m():

    bootstrap_servers = dbutils.secrets.get(scope="marketpulse", key="bootstrap_servers").strip()
    kafka_api_key = dbutils.secrets.get(scope="marketpulse", key="kafka_api_key").strip()
    kafka_api_secret = dbutils.secrets.get(scope="marketpulse", key="kafka_api_secret").strip()

    jaas_config = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        "username='{}' "
        "password='{}';"
    ).format(kafka_api_key, kafka_api_secret)

    kafka_options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": "candles.1m",
        "startingOffsets": "earliest",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas_config,
        "failOnDataLoss": "false",
    }

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
    )

    return (
        kafka_stream
        .select(
            "key",
            "value",
            "topic",
            "partition",
            "offset",
            "timestamp",
            "timestampType",
        )
        .withColumn("ingested_at", current_timestamp())
    )

