from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    max as _max,
    max_by,
    min as _min,
    min_by,
    round as _round,
    sum as _sum,
    when,
    window,
)


@dp.materialized_view(
    name="gold.market_metrics",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    cluster_by=["product_id", "window_start"]
)
def market_metrics():

    # 1. Read cleaned and deduplicated trades from Silver
    silver_trades = spark.read.table("silver.trades")

    # 2. Compute 1-hour tumbling window aggregations
    metrics = (
        silver_trades
        .groupBy(
            window("trade_time", "1 hour"),
            "product_id"
        )
        .agg(
            # OHLC Price Dynamics
            min_by("price", "trade_time").alias("open_price"),
            _max("price").alias("high_price"),
            _min("price").alias("low_price"),
            max_by("price", "trade_time").alias("close_price"),

            # Volume & Turnover
            _sum("quantity").alias("total_volume"),
            _sum(col("price") * col("quantity")).alias("total_turnover"),
            count("trade_id").alias("total_trades"),

            # Order Flow Skew (Buyer vs Seller initiated volume)
            _sum(when(col("side") == "BUY", col("quantity")).otherwise(0)).alias("buy_volume"),
            _sum(when(col("side") == "SELL", col("quantity")).otherwise(0)).alias("sell_volume"),
        )
    )

    # 3. Derive VWAP and Order Flow Ratios
    return (
        metrics
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("product_id"),
            col("open_price"),
            col("high_price"),
            col("low_price"),
            col("close_price"),
            _round(col("total_turnover") / col("total_volume"), 8).alias("vwap"),
            col("total_volume"),
            col("total_turnover"),
            col("total_trades"),
            col("buy_volume"),
            col("sell_volume"),
            _round(col("buy_volume") / col("total_volume"), 4).alias("buy_ratio"),
            current_timestamp().alias("gold_updated_at")
        )
    )

