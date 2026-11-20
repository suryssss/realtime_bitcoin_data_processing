from pyspark import pipelines as dp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, expr

# Avro schema matching schemas/candles_1m.avsc
CANDLES_AVRO_SCHEMA = """{
  "type": "record",
  "name": "candles_1m_value",
  "namespace": "org.apache.flink.avro.generated.record",
  "fields": [
    {"name": "product_id", "type": ["null", "string"], "default": null},
    {"name": "window_start", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null},
    {"name": "window_end", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null},
    {"name": "open_price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 8}], "default": null},
    {"name": "high_price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 8}], "default": null},
    {"name": "low_price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 8}], "default": null},
    {"name": "close_price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 8}], "default": null},
    {"name": "volume", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 12}], "default": null},
    {"name": "trade_count", "type": ["null", "long"], "default": null}
  ]
}"""


@dp.table(
    name="silver.candles",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    cluster_by=["product_id", "window_start"]
)
# Data Quality Expectations for OHLCV candles
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND length(trim(product_id)) > 0")
@dp.expect_or_drop("valid_window", "window_start IS NOT NULL AND window_end IS NOT NULL AND window_end > window_start")
@dp.expect_or_drop("positive_prices", "open_price > 0 AND high_price > 0 AND low_price > 0 AND close_price > 0")
@dp.expect_or_drop("high_low_boundary", "high_price >= low_price AND high_price >= open_price AND high_price >= close_price AND low_price <= open_price AND low_price <= close_price")
@dp.expect_or_drop("non_negative_volume", "volume >= 0 AND trade_count >= 0")
def candles():

    # 1. Read raw streaming table from Bronze
    raw_candles = spark.readStream.table("bronze.candles_1m")

    # 2. Guard against nulls, tombstones, and truncated payloads (< 5 bytes)
    valid_payloads = raw_candles.filter(
        col("value").isNotNull() & (expr("length(value)") > 5)
    )

    # 3. Strip Confluent 5-byte wire format header
    unwrapped = valid_payloads.withColumn(
        "avro_payload",
        expr("substring(value, 6)")
    )

    # 4. Deserialize Avro payload
    deserialized = unwrapped.withColumn(
        "candle",
        from_avro(col("avro_payload"), CANDLES_AVRO_SCHEMA, options={"mode": "PERMISSIVE"})
    )

    # 5. Project typed fields and lineage
    typed_candles = deserialized.select(
        col("candle.product_id").cast("string").alias("product_id"),
        col("candle.window_start").alias("window_start"),
        col("candle.window_end").alias("window_end"),
        col("candle.open_price").alias("open_price"),
        col("candle.high_price").alias("high_price"),
        col("candle.low_price").alias("low_price"),
        col("candle.close_price").alias("close_price"),
        col("candle.volume").alias("volume"),
        col("candle.trade_count").alias("trade_count"),
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
        typed_candles
        .withWatermark("window_end", "1 hour")
        .dropDuplicatesWithinWatermark(["product_id", "window_start", "window_end"])
    )

