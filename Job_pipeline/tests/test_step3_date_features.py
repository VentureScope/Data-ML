from __future__ import annotations

import unittest

from Job_pipeline.preprocessing.date_features import DateFeaturesModule
from Job_pipeline.tests.test_utils import load_sample_row


class TestStep3DateFeatures(unittest.TestCase):
    def test_date_feature_outputs(self) -> None:
        row = load_sample_row()
        module = DateFeaturesModule()
        out = module.transform(row)

        for key in ["timestamp", "year", "month", "year_month", "week", "quarter", "holiday_flag"]:
            self.assertIn(key, out)

        self.assertIsInstance(out["month"], int)
        self.assertIsInstance(out["holiday_flag"], bool)
        self.assertRegex(out["year_month"], r"^\d{4}-\d{2}$")


if __name__ == "__main__":
    unittest.main()
