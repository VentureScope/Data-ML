"""Shared semantic embedding utilities for preprocessing modules.

This module centralizes SentenceTransformer model loading and cosine similarity
operations so Step 4/5/10 modules use consistent production-grade embeddings.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Dict, List, Sequence

import numpy as np
from sentence_transformers import SentenceTransformer


logger = logging.getLogger(__name__)


_MODEL_CACHE: Dict[str, SentenceTransformer] = {}


@dataclass
class SemanticEncoder:
    """Lazy-loading semantic encoder using sentence-transformers."""

    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"

    def _get_model(self) -> SentenceTransformer:
        model = _MODEL_CACHE.get(self.model_name)
        if model is None:
            logger.info("Loading semantic model: %s", self.model_name)
            model = SentenceTransformer(self.model_name)
            _MODEL_CACHE[self.model_name] = model
        else:
            logger.debug("Using cached semantic model: %s", self.model_name)
        return model

    def encode(self, texts: Sequence[str]) -> np.ndarray:
        """Encode one or many texts to normalized dense vectors."""
        model = self._get_model()
        logger.debug("Encoding %d texts with model=%s", len(texts), self.model_name)
        vectors = model.encode(
            list(texts),
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        return vectors

    def encode_one(self, text: str) -> np.ndarray:
        """Encode a single text and return 1D vector."""
        return self.encode([text])[0]

    def cosine_similarities(self, query_vec: np.ndarray, candidate_vecs: np.ndarray) -> np.ndarray:
        """Compute cosine similarity between one query and many candidates."""
        # Vectors are already normalized, so dot product is cosine similarity.
        return np.dot(candidate_vecs, query_vec)


__all__ = ["SemanticEncoder"]
