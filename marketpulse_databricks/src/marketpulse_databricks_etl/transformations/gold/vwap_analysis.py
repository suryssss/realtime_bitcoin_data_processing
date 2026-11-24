from pyspark import pipelines as dp
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    lag,
    max as _max,
    max_by,
    min as _min,
    min_by,
    round as _round,
    sum as _sum,
    when,
    window,
)
from pyspark.sql.window import Window


@dp.materialized_view(
    name="gold.vwap_analysis",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    cluster_by=["product_id", "window_start"]
)
def vwap_analysis():

    # 1. Read clean 5-minute VWAP from Silver
    vwap_df = spark.read.table("silver.vwap")

    # 2. Read 1-minute OHLCV candles from Silver and aggregate to 5-minute boundaries
    candles_df = spark.read.table("silver.candles")

    candles_5m = (
        candles_df
        .groupBy(
            window("window_start", "5 minutes").alias("w"),
            "product_id"
        )
        .agg(
            min_by("open_price", "window_start").alias("candle_open"),
            _max("high_price").alias("candle_high"),
            _min("low_price").alias("candle_low"),
            max_by("close_price", "window_start").alias("candle_close"),
            _sum("volume").alias("candle_volume"),
            _sum("trade_count").alias("candle_trade_count")
        )
        .select(
            col("product_id"),
            col("w.start").alias("window_start"),
            col("candle_open"),
            col("candle_high"),
            col("candle_low"),
            col("candle_close"),
            col("candle_volume"),
            col("candle_trade_count")
        )
    )

    # 3. Equi-join on (product_id, window_start) with silver.vwap as driver
    joined = (
        vwap_df.alias("v")
        .join(
            candles_5m.alias("c"),
            on=["product_id", "window_start"],
            how="left"
        )
    )

    # 4. Window specification for rolling trend & lag analytics
    w_product = Window.partitionBy("product_id").orderBy("window_start")

    # 5. Enrich with execution spread, basis point slippage, and institutional regimes
    enriched = (
        joined
        .select(
            col("product_id"),
            col("window_start"),
            col("v.window_end").alias("window_end"),

            # Core VWAP benchmark and candle prices
            col("v.vwap").alias("vwap"),
            coalesce(col("c.candle_open"), col("v.vwap")).alias("open_price"),
            coalesce(col("c.candle_high"), col("v.vwap")).alias("high_price"),
            coalesce(col("c.candle_low"), col("v.vwap")).alias("low_price"),
            coalesce(col("c.candle_close"), col("v.vwap")).alias("close_price"),

            # Volume, trade activity, and turnover
            col("v.total_volume").alias("total_volume"),
            col("v.trade_count").alias("trade_count"),
            _round(col("v.vwap") * col("v.total_volume"), 2).alias("total_turnover"),

            # Execution spread against VWAP benchmark (Close Price - VWAP)
            _round(coalesce(col("c.candle_close"), col("v.vwap")) - col("v.vwap"), 4).alias("price_vwap_spread"),

            # Slippage in basis points: ((Close - VWAP) / VWAP) * 10000
            _round(
                ((coalesce(col("c.candle_close"), col("v.vwap")) - col("v.vwap")) / col("v.vwap")) * 10000,
                2
            ).alias("slippage_bps"),

            # Average trade sizing
            when(col("v.trade_count") > 0, _round(col("v.total_volume") / col("v.trade_count"), 8))
            .otherwise(0.0)
            .alias("avg_trade_size"),

            # Lagged VWAP for trend detection
            lag("v.vwap", 1).over(w_product).alias("prev_vwap")
        )
    )

    # 6. Final regime classification and momentum
    return (
        enriched
        .select(
            col("product_id"),
            col("window_start"),
            col("window_end"),
            col("vwap"),
            col("open_price"),
            col("high_price"),
            col("low_price"),
            col("close_price"),
            col("price_vwap_spread"),
            col("slippage_bps"),
            col("total_volume"),
            col("total_turnover"),
            col("trade_count"),
            col("avg_trade_size"),

            # Execution Regime: Premium vs Discount vs Parity
            when(col("slippage_bps") > 2.0, "PREMIUM_BUYING")
            .when(col("slippage_bps") < -2.0, "DISCOUNT_SELLING")
            .otherwise("FAIR_VALUE")
            .alias("execution_regime"),

            # Trend & Momentum
            _round(col("vwap") - coalesce(col("prev_vwap"), col("vwap")), 4).alias("vwap_change"),
            when(col("prev_vwap").isNotNull() & (col("prev_vwap") > 0),
                 _round(((col("vwap") - col("prev_vwap")) / col("prev_vwap")) * 100, 4))
            .otherwise(0.0)
            .alias("vwap_change_pct"),

            when(col("vwap") > col("prev_vwap"), "UPWARD")
            .when(col("vwap") < col("prev_vwap"), "DOWNWARD")
            .otherwise("FLAT")
            .alias("vwap_trend"),

            current_timestamp().alias("gold_updated_at")
        )
    )

