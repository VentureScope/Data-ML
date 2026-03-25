from __future__ import annotations

import unittest

from Job_pipeline.preprocessing.clean_text import CleanTextModule
from Job_pipeline.tests.test_utils import load_sample_row


class TestStep1CleanText(unittest.TestCase):
    def test_clean_text_outputs(self) -> None:
        row = load_sample_row()
        module = CleanTextModule()
        out = module.transform(row.get("title"), row.get("description"))

        self.assertIn("clean_title", out)
        self.assertIn("clean_description", out)
        self.assertTrue(out["clean_title"])
        self.assertTrue(out["clean_description"])
        self.assertEqual(out["clean_title"], out["clean_title"].lower())


if __name__ == "__main__":
    unittest.main()
