"""Pure data-contract helpers shared by the crypto ingestion pipeline."""


def source_event_id(symbol, aggregate_trade_id):
    """Return the stable business key for one Binance aggregate trade."""
    normalized_symbol = str(symbol).strip().lower()
    if not normalized_symbol:
        raise ValueError("symbol must not be empty")
    if aggregate_trade_id is None:
        raise ValueError("aggregate_trade_id must not be null")
    return f"binance:aggTrade:{normalized_symbol}:{aggregate_trade_id}"
