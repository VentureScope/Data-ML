from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from Job_pipeline.run_preprocessing_pipeline import process_csv_file


class _StubPreprocessor:
    def __init__(self) -> None:
        self.calls = 0

    def preprocess_row(self, row, source_name=None):
        self.calls += 1
        title = (row.get("title") or "").lower()
        if "accountant" in title:
            return None
        return {
            "year_month": "2026-04",
            "timestamp": "2026-04-01T00:00:00",
            "month": 4,
            "holiday_flag": False,
            "job_id": f"id-{self.calls}",
            "job_title": row.get("title", ""),
            "normalized_title": "Software Engineer",
            "DescriptionVec": [0.1, 0.2],
            "city": "Addis Ababa",
            "region": "Addis Ababa",
            "country": "Ethiopia",
            "is_remote": False,
            "job_type": "Full-time",
            "education_level": "Bachelor",
            "skills": ["Python"],
        }


class TestRunPreprocessingPipeline(unittest.TestCase):
    def test_process_csv_file_skips_non_tech_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_csv = Path(tmpdir) / "input.csv"
            output_csv = Path(tmpdir) / "output.csv"

            input_csv.write_text(
                "title,description,created_at,entity_name,city,country\n"
                "Backend Developer,Build APIs,2026-04-01,Acme,Addis Ababa,Ethiopia\n"
                "Accountant,Handle financial records,2026-04-01,Acme,Addis Ababa,Ethiopia\n",
                encoding="utf-8",
            )

            preprocessor = _StubPreprocessor()
            kept, skipped = process_csv_file(
                input_csv=input_csv,
                output_csv=output_csv,
                preprocessor=preprocessor,
            )

            self.assertEqual(kept, 1)
            self.assertEqual(skipped, 1)

            lines = output_csv.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 2)


if __name__ == "__main__":
    unittest.main()
