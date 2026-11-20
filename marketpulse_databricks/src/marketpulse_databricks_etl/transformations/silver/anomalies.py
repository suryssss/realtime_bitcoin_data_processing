from pyspark import pipelines as dp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, expr, to_timestamp

# Avro schema matching schemas/trades_anomalies.avsc
ANOMALIES_AVRO_SCHEMA = """{
  "type": "record",
  "name": "trades_anomalies_sink_value",
  "namespace": "org.apache.flink.avro.generated.record",
  "fields": [
    {"name": "trade_id", "type": ["null", "string"], "default": null},
    {"name": "product_id", "type": ["null", "string"], "default": null},
    {"name": "trade_time", "type": ["null", "string"], "default": null},
    {"name": "price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 8}], "default": null},
    {"name": "quantity", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 12}], "default": null},
    {"name": "trade_value", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 8}], "default": null},
    {"name": "anomaly_type", "type": ["null", "string"], "default": null},
    {"name": "threshold", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 8}], "default": null}
  ]
}"""


@dp.table(
    name="silver.anomalies",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    cluster_by=["product_id", "trade_time"]
)
# Data Quality Expectations for Anomalous / Large Trades
@dp.expect_or_drop("valid_trade_id", "trade_id IS NOT NULL AND length(trim(trade_id)) > 0")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND length(trim(product_id)) > 0")
@dp.expect_or_drop("valid_anomaly_type", "anomaly_type IS NOT NULL AND length(trim(anomaly_type)) > 0")
@dp.expect_or_drop("positive_price", "price > 0")
@dp.expect_or_drop("positive_quantity", "quantity > 0")
@dp.expect_or_drop("positive_trade_value", "trade_value > 0")
@dp.expect_or_drop("valid_trade_time", "trade_time IS NOT NULL")
def anomalies():

    # 1. Read raw streaming table from Bronze
    raw_anomalies = spark.readStream.table("bronze.trades_anomalies")

    # 2. Guard against nulls, tombstones, and truncated payloads (< 5 bytes)
    valid_payloads = raw_anomalies.filter(
        col("value").isNotNull() & (expr("length(value)") > 5)
    )

    # 3. Strip Confluent 5-byte wire format header
    unwrapped = valid_payloads.withColumn(
        "avro_payload",
        expr("substring(value, 6)")
    )

    # 4. Deserialize Avro payload
    deserialized = unwrapped.withColumn(
        "anomaly",
        from_avro(col("avro_payload"), ANOMALIES_AVRO_SCHEMA, options={"mode": "PERMISSIVE"})
    )

    # 5. Project typed fields and lineage
    typed_anomalies = deserialized.select(
        col("anomaly.trade_id").cast("string").alias("trade_id"),
        col("anomaly.product_id").cast("string").alias("product_id"),
        to_timestamp(col("anomaly.trade_time")).alias("trade_time"),
        col("anomaly.price").alias("price"),
        col("anomaly.quantity").alias("quantity"),
        col("anomaly.trade_value").alias("trade_value"),
        col("anomaly.anomaly_type").cast("string").alias("anomaly_type"),
        col("anomaly.threshold").alias("threshold"),
        # Lineage metadata
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("ingested_at").alias("bronze_ingested_at"),
        current_timestamp().alias("silver_processed_at")
    )

    # 6. Event-time watermarking & bounded deduplication by trade_id
    return (
        typed_anomalies
        .withWatermark("trade_time", "1 hour")
        .dropDuplicatesWithinWatermark(["trade_id"])
    )

