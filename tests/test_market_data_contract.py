"""Unit tests for the symbol universe and Binance Kline pagination contract."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "apps"))

from backfill_binance_klines import INTERVAL_MS, fetch_symbol_klines, normalize_kline
from market_symbols import DEFAULT_SYMBOLS, parse_symbols


def kline(open_time_ms, close_time_ms=None):
    return [
        open_time_ms, "100.0", "102.0", "99.0", "101.0", "12.5",
        close_time_ms if close_time_ms is not None else open_time_ms + INTERVAL_MS - 1,
        "1262.5", 42, "7.0", "707.0", "0",
    ]


class SymbolUniverseTests(unittest.TestCase):
    def test_default_universe_has_the_approved_ten_unique_usdt_symbols(self):
        self.assertEqual(len(DEFAULT_SYMBOLS), 10)
        self.assertEqual(len(set(DEFAULT_SYMBOLS)), 10)
        self.assertEqual(parse_symbols(",".join(DEFAULT_SYMBOLS), required_count=10), DEFAULT_SYMBOLS)

    def test_symbols_are_normalized(self):
        self.assertEqual(parse_symbols(" BTCUSDT, ethusdt "), ("btcusdt", "ethusdt"))

    def test_duplicate_or_non_usdt_symbols_are_rejected(self):
        with self.assertRaises(ValueError):
            parse_symbols("btcusdt,BTCUSDT")
        with self.assertRaises(ValueError):
            parse_symbols("btcfdusd")


class KlineContractTests(unittest.TestCase):
    def test_normalize_skips_current_open_candle(self):
        cutoff = 1_200_000
        self.assertIsNone(normalize_kline("btcusdt", kline(1_140_000, cutoff), cutoff))

    def test_normalize_creates_typed_closed_kline(self):
        row = normalize_kline("BTCUSDT", kline(1_080_000), 1_200_000)
        self.assertEqual(row[0], "btcusdt")
        self.assertEqual(row[1], 1_080_000)
        self.assertEqual(row[6], 101.0)
        self.assertEqual(row[9], 42)

    def test_fetch_advances_after_empty_page_and_keeps_closed_rows(self):
        requests = []

        def fake_request(symbol, start_ms, end_ms, max_retries=5):
            requests.append((symbol, start_ms, end_ms, max_retries))
            if start_ms == 0:
                return []
            return [kline(start_ms), kline(start_ms + INTERVAL_MS)]

        rows = list(
            fetch_symbol_klines(
                "btcusdt", 0, 2 * 1000 * INTERVAL_MS,
                request_delay_seconds=0, request_fn=fake_request, cutoff_ms=3 * 1000 * INTERVAL_MS,
            )
        )
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[1][1], 1000 * INTERVAL_MS)
        self.assertEqual(requests[0][3], 5)
        self.assertEqual([row[1] for row in rows], [1000 * INTERVAL_MS, 1001 * INTERVAL_MS])


if __name__ == "__main__":
    unittest.main()
