CREATE TABLE IF NOT EXISTS default.gold_crypto_analytics_v2 (
    symbol LowCardinality(String),
    window_start DateTime,
    window_end DateTime,
    avg_price Float64,
    low_price Float64,
    high_price Float64,
    total_volume Float64,
    trade_count UInt64,
    vwap Float64,
    price_change_pct Float64,
    event_date Date,
    batch_id UInt64,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (symbol, window_start);

CREATE TABLE IF NOT EXISTS default.crypto_price_alerts (
    alert_id String,
    symbol LowCardinality(String),
    window_start DateTime,
    window_end DateTime,
    observed_price Float64,
    price_change_pct Float64,
    threshold_pct Float64,
    direction LowCardinality(String),
    severity LowCardinality(String),
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (symbol, window_start, alert_id);
