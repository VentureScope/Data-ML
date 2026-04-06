from __future__ import annotations

import unittest

from Job_pipeline.preprocessing.clean_text import CleanTextModule
from Job_pipeline.preprocessing.description_embedding import DescriptionEmbeddingModule
from Job_pipeline.tests.test_utils import load_sample_row


class TestStep5DescriptionEmbedding(unittest.TestCase):
    def test_description_embedding_outputs(self) -> None:
        row = load_sample_row()
        cleaner = CleanTextModule()
        cleaned = cleaner.transform(row.get("title"), row.get("description"))

        module = DescriptionEmbeddingModule()
        out = module.transform({"clean_description": cleaned["clean_description"]})

        self.assertIn("DescriptionVec", out)
        self.assertIn("embedding_dim", out)
        self.assertIn("embedding_model", out)
        self.assertEqual(len(out["DescriptionVec"]), out["embedding_dim"])
        self.assertGreaterEqual(out["embedding_dim"], 300)
        self.assertIn("sentence-transformers", out["embedding_model"])


if __name__ == "__main__":
    unittest.main()
