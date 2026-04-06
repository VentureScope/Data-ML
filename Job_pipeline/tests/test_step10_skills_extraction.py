from __future__ import annotations

import unittest

from Job_pipeline.preprocessing.clean_text import CleanTextModule
from Job_pipeline.preprocessing.skills_extraction import SkillsExtractionModule
from Job_pipeline.tests.test_utils import load_sample_row


class TestStep10SkillsExtraction(unittest.TestCase):
    def test_skills_outputs(self) -> None:
        row = load_sample_row()
        cleaner = CleanTextModule()
        cleaned = cleaner.transform(row.get("title"), row.get("description"))

        module = SkillsExtractionModule()
        out = module.extract(cleaned["clean_description"])

        self.assertIn("skills", out)
        self.assertIn("skills_count", out)
        self.assertIn("confidence_score", out)
        self.assertIn("method_used", out)
        self.assertIsInstance(out["skills"], list)
        self.assertEqual(out["skills_count"], len(out["skills"]))
        self.assertIn("embedding", str(out["method_used"]))


if __name__ == "__main__":
    unittest.main()
