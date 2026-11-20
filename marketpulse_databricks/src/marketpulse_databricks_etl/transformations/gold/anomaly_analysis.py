from pyspark import pipelines as dp
from pyspark.sql.functions import (
    avg as _avg,
    col,
    count,
    current_timestamp,
    max as _max,
    min as _min,
    round as _round,
    sum as _sum,
    window,
)


@dp.materialized_view(
    name="gold.anomaly_analysis",
    table_properties={"quality": "gold"}
)
def anomaly_analysis():

    # 1. Read clean anomalies from Silver
    anomalies = spark.read.table("silver.anomalies")

    # 2. Aggregate anomalies by 1-hour window, product, and anomaly type
    return (
        anomalies
        .groupBy(
            window("trade_time", "1 hour"),
            "product_id",
            "anomaly_type"
        )
        .agg(
            count("trade_id").alias("anomaly_count"),
            _sum("quantity").alias("total_anomalous_volume"),
            _sum("trade_value").alias("total_anomalous_value"),
            _round(_avg("trade_value"), 2).alias("avg_anomaly_value"),
            _max("trade_value").alias("max_anomaly_value"),
            _min("price").alias("min_anomaly_price"),
            _max("price").alias("max_anomaly_price"),
            _max("trade_time").alias("latest_anomaly_time")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("product_id"),
            col("anomaly_type"),
            col("anomaly_count"),
            col("total_anomalous_volume"),
            col("total_anomalous_value"),
            col("avg_anomaly_value"),
            col("max_anomaly_value"),
            col("min_anomaly_price"),
            col("max_anomaly_price"),
            col("latest_anomaly_time"),
            current_timestamp().alias("gold_updated_at")
        )
    )

