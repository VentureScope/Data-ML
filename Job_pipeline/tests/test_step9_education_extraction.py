from __future__ import annotations

import unittest

from Job_pipeline.preprocessing.clean_text import CleanTextModule
from Job_pipeline.preprocessing.education_extraction import EducationExtractionModule
from Job_pipeline.tests.test_utils import load_sample_row


class TestStep9EducationExtraction(unittest.TestCase):
    def test_education_outputs(self) -> None:
        row = load_sample_row()
        cleaner = CleanTextModule()
        cleaned = cleaner.transform(row.get("title"), row.get("description"))

        module = EducationExtractionModule()
        out = module.extract(cleaned["clean_title"], cleaned["clean_description"])

        self.assertIn("education_level", out)
        self.assertIn("confidence_score", out)
        self.assertIn("method_used", out)
        self.assertIn(out["education_level"], {"PhD", "Masters", "Bachelors", "Diploma", "Not specified"})


if __name__ == "__main__":
    unittest.main()
