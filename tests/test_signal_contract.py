"""Unit tests for fail-safe, read-only prediction signal guardrails."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "apps"))

from signal_contract import NO_TRADE, UP, guarded_signal


class SignalGuardrailTests(unittest.TestCase):
    def base_kwargs(self):
        return {
            "probability_up": 0.75,
            "probability_down": 0.25,
            "expected_return_pct": 0.8,
            "data_age_seconds": 10,
            "max_data_age_seconds": 120,
            "feature_complete": True,
            "model_approved": True,
            "drift_detected": False,
            "volatility_pct": 0.3,
            "high_volatility_pct": 2.0,
            "min_expected_edge_pct": 0.4,
        }

    def test_approved_fresh_complete_signal_can_be_directional(self):
        action, tier, reason = guarded_signal(**self.base_kwargs())
        self.assertEqual(action, UP)
        self.assertEqual(reason, "approved")
        self.assertIn(tier, {"LOW", "MEDIUM"})

    def test_unapproved_model_blocks_signal(self):
        values = self.base_kwargs()
        values["model_approved"] = False
        self.assertEqual(guarded_signal(**values), (NO_TRADE, "HIGH", "model_not_approved"))

    def test_stale_or_missing_or_drifted_data_blocks_signal(self):
        for field, value, reason in [
            ("data_age_seconds", 121, "stale_data"),
            ("feature_complete", False, "missing_features"),
            ("drift_detected", True, "feature_drift"),
            ("volatility_pct", 2.0, "high_volatility"),
        ]:
            with self.subTest(field=field):
                values = self.base_kwargs()
                values[field] = value
                action, tier, actual_reason = guarded_signal(**values)
                self.assertEqual(action, NO_TRADE)
                self.assertEqual(tier, "HIGH")
                self.assertEqual(actual_reason, reason)

    def test_edge_must_strictly_exceed_cost_assumption(self):
        values = self.base_kwargs()
        values["expected_return_pct"] = 0.4
        action, _, reason = guarded_signal(**values)
        self.assertEqual(action, NO_TRADE)
        self.assertEqual(reason, "insufficient_net_edge")


if __name__ == "__main__":
    unittest.main()
