"""Unit tests for append-only model registry promotion and rollback rules."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "apps"))

from model_registry import APPROVED, CANDIDATE, RETIRED, approved_for, promote, register_candidate, rollback


class ModelRegistryTests(unittest.TestCase):
    def candidate(self, model_id="candidate-15m", horizon_minutes=15):
        return register_candidate(
            model_id=model_id,
            model_path=f"s3a://crypto-lake/models/candidates/{model_id}",
            feature_version="v1",
            horizon_minutes=horizon_minutes,
            validation_accuracy=0.61,
            holdout_accuracy=0.59,
            expected_return_pct=0.72,
            registered_at="2026-07-24T00:00:00+00:00",
        )

    def test_registration_is_candidate_not_live_model(self):
        candidate = self.candidate()
        self.assertEqual(candidate.status, CANDIDATE)
        self.assertIsNone(approved_for((candidate,), "v1", 15))

    def test_promotion_appends_approval_without_mutating_candidate(self):
        candidate = self.candidate()
        registry = promote((candidate,), candidate.model_id, approved_at="2026-07-24T01:00:00+00:00")
        self.assertEqual(len(registry), 2)
        self.assertEqual(registry[0], candidate)
        self.assertEqual(approved_for(registry, "v1", 15).status, APPROVED)

    def test_promotion_retires_existing_champion_for_same_contract(self):
        first = self.candidate("first")
        second = self.candidate("second")
        registry = promote((first, second), first.model_id, approved_at="2026-07-24T01:00:00+00:00")
        registry = promote(registry, second.model_id, approved_at="2026-07-24T02:00:00+00:00")
        champion = approved_for(registry, "v1", 15)
        self.assertEqual(champion.model_id, "second")
        self.assertTrue(any(record.model_id == "first" and record.status == RETIRED for record in registry))

    def test_rollback_reapproves_prior_model_without_mutating_history(self):
        first = self.candidate("first")
        second = self.candidate("second")
        registry = promote((first, second), first.model_id, approved_at="2026-07-24T01:00:00+00:00")
        registry = promote(registry, second.model_id, approved_at="2026-07-24T02:00:00+00:00")
        restored = rollback(registry, "first", approved_at="2026-07-24T03:00:00+00:00")
        self.assertEqual(approved_for(restored, "v1", 15).model_id, "first")
        self.assertGreater(len(restored), len(registry))

    def test_invalid_horizon_is_rejected(self):
        with self.assertRaises(ValueError):
            self.candidate(horizon_minutes=5)


if __name__ == "__main__":
    unittest.main()
