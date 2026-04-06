from __future__ import annotations

import unittest

from Job_pipeline.preprocessing.clean_text import CleanTextModule
from Job_pipeline.preprocessing.title_normalization import TitleNormalizationModule
from Job_pipeline.tests.test_utils import load_sample_row


class TestStep4TitleNormalization(unittest.TestCase):
    def test_title_normalization_outputs(self) -> None:
        row = load_sample_row()
        cleaner = CleanTextModule()
        cleaned = cleaner.transform(row.get("title"), row.get("description"))

        module = TitleNormalizationModule()
        out = module.normalize(cleaned.get("clean_title"), cleaned.get("clean_description"))

        self.assertIn("normalized_title", out)
        self.assertIn("confidence_score", out)
        self.assertIn("method_used", out)
        self.assertTrue(str(out["normalized_title"]).strip())
        self.assertIn("embedding", str(out["method_used"]))


if __name__ == "__main__":
    unittest.main()
