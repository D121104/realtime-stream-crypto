"""Pure feature and label contracts for leak-free market prediction datasets."""

FEATURE_VERSION = "v1"
HORIZONS_MINUTES = (15, 60)
LABEL_UP = "UP"
LABEL_DOWN = "DOWN"
LABEL_NEUTRAL = "NEUTRAL"


def future_return_pct(current_close, future_close):
    """Return the price change from an as-of close to a strictly future close."""
    current = float(current_close)
    future = float(future_close)
    if current <= 0:
        raise ValueError("current_close must be positive")
    return ((future - current) / current) * 100.0


def directional_label(label_return_pct, edge_threshold_pct):
    """Classify a future return without conflating neutral/no-edge observations."""
    threshold = float(edge_threshold_pct)
    if threshold <= 0:
        raise ValueError("edge_threshold_pct must be positive")
    value = float(label_return_pct)
    if value >= threshold:
        return LABEL_UP
    if value <= -threshold:
        return LABEL_DOWN
    return LABEL_NEUTRAL


def label_from_closes(current_close, future_close, edge_threshold_pct):
    """Produce return and class only when the horizon's future close is available."""
    if future_close is None:
        return None, None
    result = future_return_pct(current_close, future_close)
    return result, directional_label(result, edge_threshold_pct)


def split_id(as_of_epoch_ms, train_end_epoch_ms, validation_end_epoch_ms):
    """Assign chronological splits without ever randomly mixing future observations."""
    timestamp = int(as_of_epoch_ms)
    if timestamp <= int(train_end_epoch_ms):
        return "train"
    if timestamp <= int(validation_end_epoch_ms):
        return "validation"
    return "holdout"


def history_is_sufficient(history_size, required_size):
    """State whether a rolling feature can be calculated only from prior/current data."""
    return int(history_size) >= int(required_size)
