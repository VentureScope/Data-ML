from __future__ import annotations

import unittest

from Job_pipeline.preprocessing.job_id import JobIdConfig, JobIdModule
from Job_pipeline.tests.test_utils import load_sample_row


class TestStep2JobId(unittest.TestCase):
    def test_job_id_generation(self) -> None:
        row = load_sample_row()
        module = JobIdModule(
            JobIdConfig(
                title_key="title",
                company_key="entity_name",
                date_key="created_at",
                source_key="source",
                hash_length=16,
            )
        )
        out = module.transform(row)

        self.assertIn("job_id", out)
        self.assertEqual(len(out["job_id"]), 16)
        self.assertTrue(out["job_id"].isalnum())


if __name__ == "__main__":
    unittest.main()
