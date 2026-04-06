from __future__ import annotations

import unittest

from Job_pipeline.preprocessing.clean_text import CleanTextModule
from Job_pipeline.preprocessing.remote_detection import RemoteDetectionModule
from Job_pipeline.tests.test_utils import load_sample_row


class TestStep7RemoteDetection(unittest.TestCase):
    def test_remote_outputs(self) -> None:
        row = load_sample_row()
        cleaner = CleanTextModule()
        cleaned = cleaner.transform(row.get("title"), row.get("description"))

        module = RemoteDetectionModule()
        out = module.detect(cleaned["clean_title"], cleaned["clean_description"])

        self.assertIn("is_remote", out)
        self.assertIn("remote_mode", out)
        self.assertIn(out["remote_mode"], {"remote", "hybrid", "onsite", "unknown"})
        self.assertIsInstance(out["is_remote"], bool)


if __name__ == "__main__":
    unittest.main()
