"""Step 5: Description embedding module.

Purpose:
- Convert cleaned job descriptions into fixed-size vectors for similarity,
  clustering, downstream extraction, and forecasting features.

Pipeline behavior:
1. Load sentence-transformers model
2. Generate dense semantic embedding for description text
3. Return vector and embedding metadata

Main output:
- DescriptionVec (plus metadata: embedding_dim, embedding_model)

Usage:
- `DescriptionEmbeddingModule().transform(record)` for record enrichment.
- `embed_description(text)` for one-off embedding generation.

Notes:
- Uses sentence-transformers for production-grade semantic vectors.
- No Gemini/LLM fallback by design.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from Job_pipeline.preprocessing.semantic_utils import SemanticEncoder


logger = logging.getLogger(__name__)


@dataclass
class DescriptionEmbeddingConfig:
    """Configuration for embedding generation."""

    text_key: str = "clean_description"
    output_vector_key: str = "DescriptionVec"
    output_dim_key: str = "embedding_dim"
    output_model_key: str = "embedding_model"
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"


class DescriptionEmbeddingModule:
    """Class-based semantic embedding generator for descriptions."""

    def __init__(self, config: Optional[DescriptionEmbeddingConfig] = None):
        self.config = config or DescriptionEmbeddingConfig()
        self._encoder = SemanticEncoder(self.config.model_name)
        self._vector_dim = len(self._encoder.encode_one("dimension_probe").tolist())
        logger.debug("DescriptionEmbeddingModule initialized model=%s dim=%d", self.config.model_name, self._vector_dim)

    def embed_text(self, text: Optional[str]) -> List[float]:
        """Convert one description string to a fixed-size embedding vector."""
        clean = (text or "").strip()
        if not clean:
            # Keep stable output shape even for empty inputs.
            logger.debug("embed_text called with empty text, returning zero-vector")
            return [0.0] * self._vector_dim
        vector = self._encoder.encode_one(clean)
        vec_list = [float(v) for v in vector.tolist()]
        logger.info("embed_text produced vector len=%d for text_len=%d", len(vec_list), len(clean))
        return vec_list

    def transform(self, record: Dict[str, str]) -> Dict[str, object]:
        """Return a copy of record with description embedding fields attached."""
        output: Dict[str, object] = dict(record)
        embedding = self.embed_text(record.get(self.config.text_key))
        output[self.config.output_vector_key] = embedding
        output[self.config.output_dim_key] = len(embedding)
        output[self.config.output_model_key] = self.config.model_name
        return output


def embed_description(
    text: Optional[str],
    config: Optional[DescriptionEmbeddingConfig] = None,
) -> List[float]:
    """Convenience function for one-off description embedding."""
    module = DescriptionEmbeddingModule(config=config)
    return module.embed_text(text)


__all__ = [
    "DescriptionEmbeddingConfig",
    "DescriptionEmbeddingModule",
    "embed_description",
]
