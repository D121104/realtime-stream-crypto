"""Symbol-universe helpers for Binance Spot market-data jobs."""

DEFAULT_SYMBOLS = (
    "btcusdt",
    "ethusdt",
    "solusdt",
    "bnbusdt",
    "xrpusdt",
    "dogeusdt",
    "adausdt",
    "trxusdt",
    "avaxusdt",
    "linkusdt",
)


def parse_symbols(raw_symbols, required_count=None):
    """Normalize a comma-separated Binance Spot symbol list and reject duplicates."""
    symbols = tuple(symbol.strip().lower() for symbol in str(raw_symbols).split(",") if symbol.strip())
    if not symbols:
        raise ValueError("at least one symbol is required")
    if len(set(symbols)) != len(symbols):
        raise ValueError("symbols must be unique")
    if any(not symbol.endswith("usdt") or not symbol.isalnum() for symbol in symbols):
        raise ValueError("symbols must be alphanumeric USDT pairs")
    if required_count is not None and len(symbols) != required_count:
        raise ValueError(f"exactly {required_count} symbols are required")
    return symbols


def configured_symbols(raw_symbols=None):
    """Return the configured universe or the approved ten-symbol MVP default."""
    return parse_symbols(",".join(DEFAULT_SYMBOLS) if raw_symbols is None else raw_symbols)
