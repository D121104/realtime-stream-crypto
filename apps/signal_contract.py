"""Pure safety guardrails for read-only market prediction signals."""

NO_TRADE = "NO_TRADE"
UP = "UP"
DOWN = "DOWN"


def risk_tier(confidence, volatility_pct, high_volatility_pct):
    """Assign an interpretable tier without turning a prediction into a trade order."""
    if volatility_pct is None or float(volatility_pct) >= float(high_volatility_pct):
        return "HIGH"
    if float(confidence) >= 0.70:
        return "MEDIUM"
    return "LOW"


def guarded_signal(
    probability_up,
    probability_down,
    expected_return_pct,
    *,
    data_age_seconds,
    max_data_age_seconds,
    feature_complete,
    model_approved,
    drift_detected,
    volatility_pct,
    high_volatility_pct,
    min_expected_edge_pct,
):
    """Return action, risk tier, and audit reason; unsafe conditions always block."""
    if not model_approved:
        return NO_TRADE, "HIGH", "model_not_approved"
    if data_age_seconds is None or float(data_age_seconds) > float(max_data_age_seconds):
        return NO_TRADE, "HIGH", "stale_data"
    if not feature_complete:
        return NO_TRADE, "HIGH", "missing_features"
    if drift_detected:
        return NO_TRADE, "HIGH", "feature_drift"
    if volatility_pct is None or float(volatility_pct) >= float(high_volatility_pct):
        return NO_TRADE, "HIGH", "high_volatility"
    up, down = float(probability_up), float(probability_down)
    if not 0 <= up <= 1 or not 0 <= down <= 1:
        raise ValueError("probabilities must be between zero and one")
    confidence = max(up, down)
    if float(expected_return_pct) <= float(min_expected_edge_pct):
        return NO_TRADE, risk_tier(confidence, volatility_pct, high_volatility_pct), "insufficient_net_edge"
    if up == down:
        return NO_TRADE, risk_tier(confidence, volatility_pct, high_volatility_pct), "tied_probability"
    return (UP if up > down else DOWN), risk_tier(confidence, volatility_pct, high_volatility_pct), "approved"
