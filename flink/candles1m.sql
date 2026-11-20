INSERT INTO `candles.1m`
SELECT
    product_id,
    window_start,
    window_end,
    FIRST_VALUE(CAST(price AS DECIMAL(20, 8))) AS open_price,
    
    MAX(CAST(price AS DECIMAL(20, 8))) AS high_price,

    MIN(CAST(price AS DECIMAL(20, 8))) AS low_price,

    LAST_VALUE(CAST(price AS DECIMAL(20, 8))) AS close_price,

    SUM(CAST(quantity AS DECIMAL(20, 12))) AS volume,

    COUNT(*) AS trade_count
FROM TABLE(
    TUMBLE(
        TABLE `trades.raw`,                 
        DESCRIPTOR(event_time_ms),          
        INTERVAL '1' MINUTE
    )
)
GROUP BY 
    product_id, 
    window_start, 
    window_end;