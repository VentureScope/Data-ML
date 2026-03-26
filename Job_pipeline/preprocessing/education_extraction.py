"""Step 9: Education extraction module.

Purpose:
- Extract minimum education requirement level from job text.

Pipeline behavior:
1. Regex-first detection for PhD, Masters, Bachelors, Diploma
2. Rule confidence scoring with level precedence
3. Gemini fallback when no regex evidence exists

Main outputs:
- education_level
- confidence_score
- method_used

Usage:
- `EducationExtractionModule().transform(record)` for record enrichment.
- `extract_education(title, description)` for one-off usage.

Notes:
- Uses Gemini fallback only when regex rules find no education signal.
- If fallback is unavailable, returns `education_level=Not specified`.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

from Job_pipeline.preprocessing.gemini_key_selector import select_random_gemini_api_key


logger = logging.getLogger(__name__)


@dataclass
class EducationExtractionConfig:
    """Configuration for education extraction."""

    title_key: str = "clean_title"
    description_key: str = "clean_description"
    output_label_key: str = "education_level"
    output_confidence_key: str = "confidence_score"
    output_method_key: str = "method_used"
    default_method_name: str = "rule"
    fallback_method_name: str = "gemini"
    fallback_confidence: str = "gemini"
    not_specified_label: str = "Not specified"


class EducationExtractionModule:
    """Class-based extractor for minimum education requirement."""

    _patterns: Dict[str, List[re.Pattern]] = {
        "PhD": [
            re.compile(r"\bph\.?d\b", re.IGNORECASE),
            re.compile(r"\bdoctorate\b", re.IGNORECASE),
            re.compile(r"\bdoctoral\b", re.IGNORECASE),
        ],
        "Masters": [
            re.compile(r"\bmaster'?s\b", re.IGNORECASE),
            re.compile(r"\bmsc\b", re.IGNORECASE),
            re.compile(r"\bm\.sc\b", re.IGNORECASE),
            re.compile(r"\bpostgraduate\b", re.IGNORECASE),
        ],
        "Bachelors": [
            re.compile(r"\bbachelor'?s\b", re.IGNORECASE),
            re.compile(r"\bbsc\b", re.IGNORECASE),
            re.compile(r"\bb\.sc\b", re.IGNORECASE),
            re.compile(r"\bundergaduate\b", re.IGNORECASE),
            re.compile(r"\bundergraduate\b", re.IGNORECASE),
            re.compile(r"\bdegree\b", re.IGNORECASE),
        ],
        "Diploma": [
            re.compile(r"\bdiploma\b", re.IGNORECASE),
            re.compile(r"\btechnical\s*diploma\b", re.IGNORECASE),
            re.compile(r"\bassociate\s*degree\b", re.IGNORECASE),
        ],
    }

    _rank = {
        "Diploma": 1,
        "Bachelors": 2,
        "Masters": 3,
        "PhD": 4,
    }

    def __init__(
        self,
        config: Optional[EducationExtractionConfig] = None,
        gemini_callable: Optional[Callable[[str], str]] = None,
    ):
        self.config = config or EducationExtractionConfig()
        self._gemini_callable = gemini_callable
        logger.debug("EducationExtractionModule initialized: %s", self.config)

    def _build_text(self, title: Optional[str], description: Optional[str]) -> str:
        return f"{title or ''}\n{description or ''}".strip()

    def _score_rules(self, text: str) -> Dict[str, int]:
        scores = {k: 0 for k in self._patterns.keys()}
        for label, pats in self._patterns.items():
            for pat in pats:
                if pat.search(text):
                    scores[label] += 1
        return scores

    def _rule_extract(self, text: str) -> Optional[Tuple[str, float]]:
        if not text:
            return None

        scores = self._score_rules(text)
        matched = [(k, v) for k, v in scores.items() if v > 0]
        if not matched:
            return None

        # Choose the highest required level when multiple appear.
        top_label = sorted(matched, key=lambda kv: (self._rank[kv[0]], kv[1]), reverse=True)[0][0]
        conf = 0.78 + min(scores[top_label] * 0.06, 0.16)
        return top_label, round(min(conf, 0.94), 2)

    def _build_gemini_prompt(self, text: str) -> str:
        return (
            "Extract minimum education requirement:\n"
            "PhD, Masters, Bachelors, Diploma, Not specified.\n\n"
            f"Job description:\n{text}"
        )

    def _normalize_gemini_label(self, raw: str) -> Optional[str]:
        value = (raw or "").strip().lower()
        if not value:
            return None

        mapping = {
            "phd": "PhD",
            "doctorate": "PhD",
            "doctoral": "PhD",
            "masters": "Masters",
            "master": "Masters",
            "msc": "Masters",
            "bachelors": "Bachelors",
            "bachelor": "Bachelors",
            "bsc": "Bachelors",
            "diploma": "Diploma",
            "not specified": "Not specified",
            "not_specified": "Not specified",
            "none": "Not specified",
        }

        if value in mapping:
            return mapping[value]

        for key, normalized in mapping.items():
            if key in value:
                return normalized

        return None

    def _call_gemini(self, prompt: str) -> Optional[str]:
        if self._gemini_callable is not None:
            try:
                return self._gemini_callable(prompt)
            except Exception:
                return None

        api_key = select_random_gemini_api_key()
        if not api_key:
            return None

        try:
            from google import genai  # type: ignore

            model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
            client = genai.Client(api_key=api_key)
            response = client.models.generate_content(model=model, contents=prompt)
            return str(getattr(response, "text", "") or "").strip()
        except Exception:
            return None

    def extract(self, title: Optional[str], description: Optional[str]) -> Dict[str, object]:
        """Extract education requirement with regex-first and fallback logic."""
        text = self._build_text(title, description)
        logger.debug("EducationExtraction.extract text_len=%d", len(text))

        rule = self._rule_extract(text)
        if rule is not None:
            label, confidence = rule
            logger.info("EducationExtraction.rule match label=%s confidence=%s", label, confidence)
            return {
                self.config.output_label_key: label,
                self.config.output_confidence_key: confidence,
                self.config.output_method_key: self.config.default_method_name,
            }

        prompt = self._build_gemini_prompt(text)
        logger.debug("EducationExtraction calling Gemini fallback")
        raw = self._call_gemini(prompt)
        label = self._normalize_gemini_label(raw or "")
        if label is not None:
            logger.info("EducationExtraction.gemini fallback label=%s", label)
            return {
                self.config.output_label_key: label,
                self.config.output_confidence_key: self.config.fallback_confidence,
                self.config.output_method_key: self.config.fallback_method_name,
            }

        return {
            self.config.output_label_key: self.config.not_specified_label,
            self.config.output_confidence_key: 0.0,
            self.config.output_method_key: f"{self.config.default_method_name}_fallback_unavailable",
        }

    def transform(self, record: Dict[str, str]) -> Dict[str, object]:
        """Return record copy with extracted education fields attached."""
        output: Dict[str, object] = dict(record)
        output.update(
            self.extract(
                title=record.get(self.config.title_key),
                description=record.get(self.config.description_key),
            )
        )
        return output


def extract_education(
    title: Optional[str],
    description: Optional[str],
    config: Optional[EducationExtractionConfig] = None,
    gemini_callable: Optional[Callable[[str], str]] = None,
) -> Dict[str, object]:
    """Convenience function for one-off education extraction."""
    module = EducationExtractionModule(config=config, gemini_callable=gemini_callable)
    return module.extract(title=title, description=description)


__all__ = [
    "EducationExtractionConfig",
    "EducationExtractionModule",
    "extract_education",
]
