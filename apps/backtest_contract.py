"""Pure, deterministic backtest rules for directional market signals."""

ROUND_TRIP_COST_PCT = 0.40
SIDE_COST_PCT = ROUND_TRIP_COST_PCT / 2
NO_TRADE = "NO_TRADE"
UP = "UP"
DOWN = "DOWN"


def net_return_pct(direction, future_return_pct, side_cost_pct=SIDE_COST_PCT):
    """Calculate return after charging cost once to enter and once to exit."""
    cost = float(side_cost_pct)
    if cost < 0:
        raise ValueError("side_cost_pct must not be negative")
    if direction == UP:
        gross = float(future_return_pct)
    elif direction == DOWN:
        gross = -float(future_return_pct)
    elif direction == NO_TRADE:
        return 0.0
    else:
        raise ValueError("direction must be UP, DOWN, or NO_TRADE")
    return gross - (2 * cost)


def signal_action(probability_up, probability_down, expected_return_pct, min_expected_edge_pct=ROUND_TRIP_COST_PCT):
    """Choose a directional signal only when confidence and net expected edge agree."""
    up = float(probability_up)
    down = float(probability_down)
    edge = float(min_expected_edge_pct)
    expected = float(expected_return_pct)
    if not 0 <= up <= 1 or not 0 <= down <= 1:
        raise ValueError("probabilities must be between zero and one")
    if edge <= 0:
        raise ValueError("min_expected_edge_pct must be positive")
    if expected <= edge:
        return NO_TRADE
    if up > down:
        return UP
    if down > up:
        return DOWN
    return NO_TRADE


def summarize_returns(net_returns):
    """Return testable aggregate metrics for a completed backtest sequence."""
    values = [float(value) for value in net_returns]
    if not values:
        return {"trade_count": 0, "net_return_pct": 0.0, "win_rate": None, "max_drawdown_pct": 0.0}
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return {
        "trade_count": len(values),
        "net_return_pct": cumulative,
        "win_rate": sum(value > 0 for value in values) / len(values),
        "max_drawdown_pct": max_drawdown,
    }
