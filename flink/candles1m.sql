INSERT INTO `candles.1m`
SELECT
    product_id,
    window_start,
    window_end,
    -- Open: First trade price in the 1-minute window
    FIRST_VALUE(CAST(price AS DECIMAL(20, 8))) AS open_price,
    
    -- High: Highest trade price in the 1-minute window
    MAX(CAST(price AS DECIMAL(20, 8))) AS high_price,
    
    -- Low: Lowest trade price in the 1-minute window
    MIN(CAST(price AS DECIMAL(20, 8))) AS low_price,
    
    -- Close: Last trade price in the 1-minute window
    LAST_VALUE(CAST(price AS DECIMAL(20, 8))) AS close_price,
    
    -- Volume: Sum of trade quantities in the window
    SUM(CAST(quantity AS DECIMAL(20, 12))) AS volume,
    
    -- Trade Count: Total executions in the 1-minute window
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