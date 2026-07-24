"""Unit tests for conservative trading-cost and signal-selection rules."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "apps"))

from backtest_contract import DOWN, NO_TRADE, UP, net_return_pct, signal_action, summarize_returns


class BacktestContractTests(unittest.TestCase):
    def test_long_trade_charges_entry_and_exit_costs(self):
        self.assertAlmostEqual(net_return_pct(UP, 1.0), 0.6)

    def test_short_trade_charges_entry_and_exit_costs(self):
        self.assertAlmostEqual(net_return_pct(DOWN, -1.0), 0.6)

    def test_no_trade_has_no_cost(self):
        self.assertEqual(net_return_pct(NO_TRADE, 5.0), 0.0)

    def test_expected_edge_must_strictly_exceed_round_trip_cost(self):
        self.assertEqual(signal_action(0.9, 0.1, 0.4), NO_TRADE)
        self.assertEqual(signal_action(0.9, 0.1, 0.41), UP)

    def test_direction_uses_the_higher_probability(self):
        self.assertEqual(signal_action(0.2, 0.8, 1.0), DOWN)

    def test_summary_reports_cost_adjusted_drawdown(self):
        summary = summarize_returns([1.0, -2.0, 0.5])
        self.assertEqual(summary["trade_count"], 3)
        self.assertAlmostEqual(summary["net_return_pct"], -0.5)
        self.assertAlmostEqual(summary["max_drawdown_pct"], 2.0)


if __name__ == "__main__":
    unittest.main()
