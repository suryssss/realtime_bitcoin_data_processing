from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    max as _max,
    round as _round,
    sum as _sum,
    when,
    window,
)


@dp.materialized_view(
    name="gold.volume_analysis",
    table_properties={"quality": "gold"}
)
def volume_analysis():

    # 1. Read cleaned trades from Silver
    trades = spark.read.table("silver.trades")

    # 2. Segment volume into trade size tiers (Retail vs Whale) over 1-hour windows
    return (
        trades
        .groupBy(
            window("trade_time", "1 hour"),
            "product_id"
        )
        .agg(
            # Total volume and trade activity
            _sum("quantity").alias("total_volume"),
            count("trade_id").alias("total_trades"),
            _round(_sum("quantity") / count("trade_id"), 8).alias("avg_trade_size"),
            _max("quantity").alias("max_single_trade_size"),

            # Retail Tier (< 0.1 BTC)
            _sum(when(col("quantity") < 0.1, col("quantity")).otherwise(0)).alias("retail_volume"),
            count(when(col("quantity") < 0.1, 1)).alias("retail_trade_count"),

            # Mid-tier (0.1 to 1.0 BTC)
            _sum(when((col("quantity") >= 0.1) & (col("quantity") < 1.0), col("quantity")).otherwise(0)).alias("medium_volume"),
            count(when((col("quantity") >= 0.1) & (col("quantity") < 1.0), 1)).alias("medium_trade_count"),

            # Whale Tier (>= 1.0 BTC)
            _sum(when(col("quantity") >= 1.0, col("quantity")).otherwise(0)).alias("whale_volume"),
            count(when(col("quantity") >= 1.0, 1)).alias("whale_trade_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("product_id"),
            col("total_volume"),
            col("total_trades"),
            col("avg_trade_size"),
            col("max_single_trade_size"),
            col("retail_volume"),
            col("retail_trade_count"),
            col("medium_volume"),
            col("medium_trade_count"),
            col("whale_volume"),
            col("whale_trade_count"),
            _round(col("whale_volume") / col("total_volume"), 4).alias("whale_volume_ratio"),
            current_timestamp().alias("gold_updated_at")
        )
    )

