CREATE TABLE IF NOT EXISTS default.gold_crypto_analytics (
    symbol String,
    window_start DateTime,
    window_end DateTime,
    avg_price Float64,
    low_price Float64,
    high_price Float64,
    total_volume Float64,
    trade_count Int64,
    vwap Float64,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (symbol, window_start);