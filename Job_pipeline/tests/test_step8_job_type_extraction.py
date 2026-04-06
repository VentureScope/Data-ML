from __future__ import annotations

import unittest

from Job_pipeline.preprocessing.clean_text import CleanTextModule
from Job_pipeline.preprocessing.job_type_extraction import JobTypeExtractionModule
from Job_pipeline.tests.test_utils import load_sample_row


class TestStep8JobTypeExtraction(unittest.TestCase):
    def test_job_type_outputs(self) -> None:
        row = load_sample_row()
        cleaner = CleanTextModule()
        cleaned = cleaner.transform(row.get("title"), row.get("description"))

        module = JobTypeExtractionModule()
        out = module.extract(cleaned["clean_title"], cleaned["clean_description"])

        self.assertIn("job_type", out)
        self.assertIn("confidence_score", out)
        self.assertIn("method_used", out)
        self.assertIn(
            out["job_type"],
            {"full_time", "part_time", "internship", "contractual", "temporary", "freelance", "not_specified"},
        )


if __name__ == "__main__":
    unittest.main()
