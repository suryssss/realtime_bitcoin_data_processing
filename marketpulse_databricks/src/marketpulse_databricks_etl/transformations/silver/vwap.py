from pyspark import pipelines as dp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, expr

# Avro schema matching schemas/vwap_5m.avsc
VWAP_AVRO_SCHEMA = """{
  "type": "record",
  "name": "vwap_5m_value",
  "namespace": "org.apache.flink.avro.generated.record",
  "fields": [
    {"name": "product_id", "type": ["null", "string"], "default": null},
    {"name": "window_start", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null},
    {"name": "window_end", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null},
    {"name": "vwap", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 8}], "default": null},
    {"name": "total_volume", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 12}], "default": null},
    {"name": "trade_count", "type": ["null", "long"], "default": null}
  ]
}"""


@dp.table(
    name="silver.vwap",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    cluster_by=["product_id", "window_start"]
)
# Data Quality Expectations for 5-minute VWAP
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND length(trim(product_id)) > 0")
@dp.expect_or_drop("valid_window", "window_start IS NOT NULL AND window_end IS NOT NULL AND window_end > window_start")
@dp.expect_or_drop("positive_vwap", "vwap > 0")
@dp.expect_or_drop("non_negative_volume", "total_volume >= 0 AND trade_count >= 0")
def vwap():

    # 1. Read raw streaming table from Bronze
    raw_vwap = spark.readStream.table("bronze.vwap_5m")

    # 2. Guard against nulls, tombstones, and truncated payloads (< 5 bytes)
    valid_payloads = raw_vwap.filter(
        col("value").isNotNull() & (expr("length(value)") > 5)
    )

    # 3. Strip Confluent 5-byte wire format header
    unwrapped = valid_payloads.withColumn(
        "avro_payload",
        expr("substring(value, 6)")
    )

    # 4. Deserialize Avro payload
    deserialized = unwrapped.withColumn(
        "vwap_data",
        from_avro(col("avro_payload"), VWAP_AVRO_SCHEMA, options={"mode": "PERMISSIVE"})
    )

    # 5. Project typed fields and lineage
    typed_vwap = deserialized.select(
        col("vwap_data.product_id").cast("string").alias("product_id"),
        col("vwap_data.window_start").alias("window_start"),
        col("vwap_data.window_end").alias("window_end"),
        col("vwap_data.vwap").alias("vwap"),
        col("vwap_data.total_volume").alias("total_volume"),
        col("vwap_data.trade_count").alias("trade_count"),
        # Lineage metadata
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("ingested_at").alias("bronze_ingested_at"),
        current_timestamp().alias("silver_processed_at")
    )

    # 6. Event-time watermarking & bounded deduplication on composite key
    return (
        typed_vwap
        .withWatermark("window_end", "1 hour")
        .dropDuplicatesWithinWatermark(["product_id", "window_start", "window_end"])
    )

