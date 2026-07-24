"""Unit tests for the stable Binance event identity contract."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "apps"))

from event_contract import source_event_id


class SourceEventIdTests(unittest.TestCase):
    def test_is_deterministic_for_the_same_aggregate_trade(self):
        first = source_event_id("BTCUSDT", 123456789)
        second = source_event_id("BTCUSDT", 123456789)

        self.assertEqual(first, second)
        self.assertEqual(first, "binance:aggTrade:btcusdt:123456789")

    def test_normalizes_symbol_for_reconnect_and_replay_idempotency(self):
        self.assertEqual(
            source_event_id(" btcusdt ", "42"),
            source_event_id("BTCUSDT", "42"),
        )

    def test_rejects_empty_symbol(self):
        with self.assertRaises(ValueError):
            source_event_id("   ", 42)

    def test_rejects_missing_aggregate_trade_id(self):
        with self.assertRaises(ValueError):
            source_event_id("ETHUSDT", None)


if __name__ == "__main__":
    unittest.main()
