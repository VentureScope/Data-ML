from __future__ import annotations

import unittest

from Job_pipeline.preprocessing.clean_text import CleanTextModule
from Job_pipeline.preprocessing.location_extraction import LocationExtractionConfig, LocationExtractionModule
from Job_pipeline.tests.test_utils import load_sample_row


class TestStep6LocationExtraction(unittest.TestCase):
    def test_location_outputs(self) -> None:
        row = load_sample_row()
        cleaner = CleanTextModule()
        cleaned = cleaner.transform(row.get("title"), row.get("description"))

        location_field = f"{row.get('city', '')}, {row.get('country', '')}".strip(", ")
        module = LocationExtractionModule(LocationExtractionConfig(location_key="location"))
        out = module.extract(cleaned["clean_description"], location_value=location_field)

        for key in ["city", "region", "country", "confidence_score", "method_used"]:
            self.assertIn(key, out)
        self.assertTrue(out["country"])

    def test_spacy_ner_path_available(self) -> None:
        module = LocationExtractionModule()
        ner = module._extract_with_ner("This role is based in Nairobi and supports distributed teams.")
        self.assertIsNotNone(ner)


if __name__ == "__main__":
    unittest.main()
