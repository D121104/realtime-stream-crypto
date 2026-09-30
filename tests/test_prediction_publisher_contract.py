"""Tests for deterministic audit identity used when publishing signals."""

import ast
import unittest
from pathlib import Path


class PredictionPublisherContractTests(unittest.TestCase):
    def test_prediction_identity_uses_all_model_and_feature_dimensions(self):
        source = Path("apps/publish_prediction_signals.py").read_text(encoding="utf-8")
        module = ast.parse(source)
        function = next(node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "prediction_id_columns")
        returned = next(node.value for node in ast.walk(function) if isinstance(node, ast.Return))
        fields = [element.value for element in returned.elts]
        self.assertEqual(fields, ["symbol", "as_of_ts", "horizon_minutes", "feature_version", "model_id"])

    def test_publisher_remains_audit_only(self):
        source = Path("apps/publish_prediction_signals.py").read_text(encoding="utf-8")
        self.assertIn('PREDICTIONS_TABLE = "crypto_prediction_signals"', source)
        self.assertNotIn("binance", source.lower())
        self.assertNotIn("order", source.lower())


if __name__ == "__main__":
    unittest.main()
