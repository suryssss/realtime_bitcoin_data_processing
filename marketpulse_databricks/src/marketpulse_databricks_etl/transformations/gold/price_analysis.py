from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    current_timestamp,
    round as _round,
    when,
)


@dp.materialized_view(
    name="gold.price_analysis",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    cluster_by=["product_id", "window_start"]
)
def price_analysis():

    # 1. Read clean 1-minute candles from Silver
    candles = spark.read.table("silver.candles")

    # 2. Derive price momentum, volatility spread, and candlestick classification
    return (
        candles
        .select(
            col("product_id"),
            col("window_start"),
            col("window_end"),
            col("open_price"),
            col("high_price"),
            col("low_price"),
            col("close_price"),
            col("volume"),
            col("trade_count"),

            # Price spread and volatility (spread in basis points)
            (col("high_price") - col("low_price")).alias("price_spread"),
            _round(((col("high_price") - col("low_price")) / col("open_price")) * 10000, 2).alias("spread_bps"),

            # Net price change and percent return
            (col("close_price") - col("open_price")).alias("price_change"),
            _round(((col("close_price") - col("open_price")) / col("open_price")) * 100, 4).alias("price_change_pct"),

            # Candle classification for dashboard visual rendering
            when(col("close_price") > col("open_price"), "BULLISH")
            .when(col("close_price") < col("open_price"), "BEARISH")
            .otherwise("DOJI")
            .alias("candle_type"),

            # Average trade volume per candle
            when(col("trade_count") > 0, _round(col("volume") / col("trade_count"), 8))
            .otherwise(0)
            .alias("avg_trade_size"),

            current_timestamp().alias("gold_updated_at")
        )
    )

