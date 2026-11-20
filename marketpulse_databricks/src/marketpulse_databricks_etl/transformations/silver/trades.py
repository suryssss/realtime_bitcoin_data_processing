from pyspark import pipelines as dp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, expr, to_timestamp

# Avro schema matching schemas/trade.avsc
TRADE_AVRO_SCHEMA = """{
  "type": "record",
  "name": "Trade",
  "namespace": "io.marketpulse",
  "fields": [
    {"name": "trade_id", "type": "string"},
    {"name": "product_id", "type": "string"},
    {"name": "price", "type": "string"},
    {"name": "quantity", "type": "string"},
    {"name": "side", "type": "string"},
    {"name": "trade_time", "type": "string"},
    {"name": "exchange", "type": ["null", "string"], "default": null}
  ]
}"""


@dp.table(
    name="silver.trades",
    table_properties={"quality": "silver"}
)
# Data Quality Expectations: Invalid or unparseable records are dropped and logged in DLT metrics
@dp.expect_or_drop("valid_trade_id", "trade_id IS NOT NULL AND length(trim(trade_id)) > 0")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND length(trim(product_id)) > 0")
@dp.expect_or_drop("positive_price", "price > 0")
@dp.expect_or_drop("positive_quantity", "quantity > 0")
@dp.expect_or_drop("valid_side", "side IN ('BUY', 'SELL')")
@dp.expect_or_drop("valid_trade_time", "trade_time IS NOT NULL")
def trades():

    # 1. Read raw streaming table from Bronze
    raw_trades = spark.readStream.table("bronze.trades_raw")

    # 2. Guard against nulls, tombstones, and truncated payloads (< 5 bytes)
    valid_payloads = raw_trades.filter(
        col("value").isNotNull() & (expr("length(value)") > 5)
    )

    # 3. Strip Confluent 5-byte wire format header (1 magic byte + 4 bytes schema ID)
    unwrapped = valid_payloads.withColumn(
        "avro_payload",
        expr("substring(value, 6)")
    )

    # 4. Deserialize Avro payload (PERMISSIVE mode: bad records produce NULL struct)
    deserialized = unwrapped.withColumn(
        "trade",
        from_avro(col("avro_payload"), TRADE_AVRO_SCHEMA, options={"mode": "PERMISSIVE"})
    )

    # 5. Project, cast types, and preserve lineage metadata
    typed_trades = deserialized.select(
        col("trade.trade_id").cast("string").alias("trade_id"),
        col("trade.product_id").cast("string").alias("product_id"),
        col("trade.price").cast("decimal(18, 8)").alias("price"),
        col("trade.quantity").cast("decimal(18, 8)").alias("quantity"),
        col("trade.side").cast("string").alias("side"),
        to_timestamp(col("trade.trade_time")).alias("trade_time"),
        col("trade.exchange").cast("string").alias("exchange"),
        # Metadata / Lineage columns for auditability
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("ingested_at").alias("bronze_ingested_at"),
        current_timestamp().alias("silver_processed_at")
    )

    # 6. Event-time watermarking & bounded deduplication
    # Note: 1-hour horizon bounds state store memory while deduplicating live retries
    return (
        typed_trades
        .withWatermark("trade_time", "1 hour")
        .dropDuplicatesWithinWatermark(["trade_id"])
    )

