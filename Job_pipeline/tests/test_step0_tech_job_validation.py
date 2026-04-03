from __future__ import annotations

import unittest

from Job_pipeline.preprocessing.tech_job_validation import TechJobValidationModule


class TestStep0TechJobValidation(unittest.TestCase):
    def setUp(self) -> None:
        self.module = TechJobValidationModule()

    def test_classifies_clear_tech_role(self) -> None:
        out = self.module.classify(
            title="Senior Backend Developer",
            description="Build REST APIs, optimize PostgreSQL queries, and maintain microservices.",
        )
        self.assertTrue(out["is_tech"])
        self.assertIn("matched_role", out)
        self.assertIn("matched_category", out)
        self.assertIn("confidence_score", out)

    def test_rejects_clear_non_tech_role(self) -> None:
        out = self.module.classify(
            title="Office Administrator",
            description="Manage office supplies, scheduling, and front desk communication.",
        )
        self.assertFalse(out["is_tech"])


if __name__ == "__main__":
    unittest.main()
