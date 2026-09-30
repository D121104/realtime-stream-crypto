"""Regression tests for leak-free feature labels and chronological data splits."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "apps"))

from feature_contract import (
    LABEL_DOWN,
    LABEL_NEUTRAL,
    LABEL_UP,
    directional_label,
    history_is_sufficient,
    label_from_closes,
    split_id,
)


class FeatureLabelTests(unittest.TestCase):
    def test_label_uses_the_future_close_not_the_current_close(self):
        label_return, label = label_from_closes(100.0, 101.0, edge_threshold_pct=0.5)
        self.assertEqual(label_return, 1.0)
        self.assertEqual(label, LABEL_UP)

    def test_missing_future_close_has_no_label_to_prevent_tail_leakage(self):
        self.assertEqual(label_from_closes(100.0, None, edge_threshold_pct=0.5), (None, None))

    def test_neutral_and_down_boundaries_are_explicit(self):
        self.assertEqual(directional_label(0.49, 0.5), LABEL_NEUTRAL)
        self.assertEqual(directional_label(-0.5, 0.5), LABEL_DOWN)

    def test_splits_are_strictly_chronological(self):
        self.assertEqual(split_id(100, 100, 200), "train")
        self.assertEqual(split_id(101, 100, 200), "validation")
        self.assertEqual(split_id(201, 100, 200), "holdout")

    def test_feature_history_must_reach_its_rolling_window(self):
        self.assertFalse(history_is_sufficient(14, 15))
        self.assertTrue(history_is_sufficient(15, 15))


if __name__ == "__main__":
    unittest.main()
