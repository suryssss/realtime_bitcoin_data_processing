INSERT INTO `marketpulse`.`market_pulse`.`vwap.5m`
SELECT
    product_id,
    window_start,
    window_end,
    CAST(
        SUM(
            CAST(price AS DECIMAL(20, 8)) *
            CAST(quantity AS DECIMAL(20, 12))
        ) /
        SUM(CAST(quantity AS DECIMAL(20, 12)))
        AS DECIMAL(20, 8)
    ) AS vwap,
    SUM(CAST(quantity AS DECIMAL(20, 12))) AS total_volume,
    COUNT(*) AS trade_count
FROM TABLE(
    TUMBLE(
        TABLE trades_valid_time,
        DESCRIPTOR(event_time_ms),
        INTERVAL '5' MINUTE
    )
)
GROUP BY product_id, window_start, window_end;