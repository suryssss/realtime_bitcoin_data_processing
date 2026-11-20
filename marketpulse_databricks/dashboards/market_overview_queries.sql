-- ==============================================================================
-- MarketPulse Lite — Real-Time Crypto Data Engineering Platform
-- Databricks SQL / AI-BI Dashboard Queries
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- Widget 1: Executive KPI Cards (Single-value counters)
-- ------------------------------------------------------------------------------

-- 1A: Latest BTC Price & Direction
SELECT 
    close_price AS latest_price,
    price_change,
    price_change_pct,
    candle_type,
    window_end AS as_of_time
FROM marketpulse.gold.price_analysis
ORDER BY window_end DESC
LIMIT 1;

-- 1B: Total Session Volume & Trades
SELECT 
    ROUND(SUM(total_volume), 4) AS session_total_volume_btc,
    ROUND(SUM(total_turnover), 2) AS session_total_turnover_usd,
    SUM(total_trades) AS session_total_trades,
    ROUND(SUM(total_turnover) / SUM(total_volume), 2) AS session_cumulative_vwap
FROM marketpulse.gold.market_metrics;

-- 1C: Whale Activity Summary
SELECT 
    SUM(whale_trade_count) AS total_whale_trades,
    ROUND(SUM(whale_volume), 4) AS total_whale_volume_btc,
    ROUND(SUM(whale_volume) / NULLIF(SUM(total_volume), 0) * 100, 2) AS whale_volume_percentage
FROM marketpulse.gold.volume_analysis;

-- 1D: Total Anomalies Detected
SELECT 
    COUNT(*) AS total_anomalies_detected,
    ROUND(SUM(quantity), 4) AS total_anomalous_btc_volume,
    ROUND(SUM(trade_value), 2) AS total_anomalous_usd_value
FROM marketpulse.silver.anomalies;


-- ------------------------------------------------------------------------------
-- Widget 2: Candlestick Chart (1-Minute OHLC)
-- Visualization: Boxplot / Candlestick or Line Chart
-- X-Axis: window_start
-- Metrics: open_price, high_price, low_price, close_price
-- ------------------------------------------------------------------------------
SELECT 
    window_start,
    window_end,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    trade_count,
    candle_type,
    spread_bps
FROM marketpulse.gold.price_analysis
ORDER BY window_start ASC;


-- ------------------------------------------------------------------------------
-- Widget 3: Live VWAP vs. Execution Price Benchmark
-- Visualization: Multi-line chart (Market Close Price vs 5-Minute VWAP)
-- X-Axis: window_start
-- Y-Axis: close_price, vwap
-- ------------------------------------------------------------------------------
SELECT 
    c.window_start,
    c.close_price,
    v.vwap AS vwap_5m,
    ROUND(c.close_price - v.vwap, 2) AS price_vwap_spread
FROM marketpulse.gold.price_analysis c
LEFT JOIN marketpulse.silver.vwap v
    ON c.product_id = v.product_id
   AND c.window_end >= v.window_start
   AND c.window_end <= v.window_end
ORDER BY c.window_start ASC;


-- ------------------------------------------------------------------------------
-- Widget 4: Volume Segmentation — Retail vs. Whale Flow
-- Visualization: Stacked Bar Chart / Stacked Area Chart
-- X-Axis: window_start
-- Y-Axis: retail_volume, medium_volume, whale_volume
-- ------------------------------------------------------------------------------
SELECT 
    window_start,
    retail_volume,
    medium_volume,
    whale_volume,
    whale_volume_ratio * 100 AS whale_volume_pct,
    total_volume
FROM marketpulse.gold.volume_analysis
ORDER BY window_start ASC;


-- ------------------------------------------------------------------------------
-- Widget 5: Order Flow Sentiment (Buyer vs. Seller Skew)
-- Visualization: Bar Chart (with benchmark line at 0.5)
-- X-Axis: window_start
-- Y-Axis: buy_ratio (Value > 0.5 = Net Buying Pressure, < 0.5 = Net Selling Pressure)
-- ------------------------------------------------------------------------------
SELECT 
    window_start,
    buy_volume,
    sell_volume,
    total_volume,
    buy_ratio,
    CASE 
        WHEN buy_ratio > 0.55 THEN 'Bullish'
        WHEN buy_ratio < 0.45 THEN 'Bearish'
        ELSE 'Neutral'
    END AS flow_sentiment
FROM marketpulse.gold.market_metrics
ORDER BY window_start ASC;


-- ------------------------------------------------------------------------------
-- Widget 6: Trade Anomaly Scatter & Heatmap Feed
-- Visualization: Scatter Plot (X: trade_time, Y: trade_value, Color: anomaly_type)
-- ------------------------------------------------------------------------------
SELECT 
    trade_time,
    product_id,
    anomaly_type,
    trade_value,
    price,
    quantity,
    threshold,
    trade_id
FROM marketpulse.silver.anomalies
ORDER BY trade_time DESC;


-- ------------------------------------------------------------------------------
-- Widget 7: Real-Time Trade Tape (Recent Executions Feed)
-- Visualization: Table (Color-coded Side: Green for BUY, Red for SELL)
-- ------------------------------------------------------------------------------
SELECT 
    trade_time,
    product_id,
    side,
    price,
    quantity,
    ROUND(price * quantity, 2) AS trade_value_usd,
    trade_id,
    exchange
FROM marketpulse.silver.trades
ORDER BY trade_time DESC
LIMIT 50;

