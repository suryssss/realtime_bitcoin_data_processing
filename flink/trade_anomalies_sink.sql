INSERT INTO `marketpulse`.`market_pulse`.`trades_anomalies_sink`
SELECT
    trade_id,
    product_id,
    trade_time,
    CAST(price AS DECIMAL(20, 8)) AS price,
    CAST(quantity AS DECIMAL(20, 12)) AS quantity,
    CAST(
        CAST(price AS DECIMAL(20, 8)) *
        CAST(quantity AS DECIMAL(20, 12))
        AS DECIMAL(20, 8)
    ) AS trade_value,
    'LARGE_TRADE' AS anomaly_type,
    CAST(100000 AS DECIMAL(20, 8)) AS threshold
FROM `marketpulse`.`market_pulse`.`trades_valid_time`
WHERE
    CAST(price AS DECIMAL(20, 8)) *
    CAST(quantity AS DECIMAL(20, 12)) > 100000;