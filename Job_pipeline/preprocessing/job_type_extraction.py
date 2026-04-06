"""Step 8: Job type extraction module.

Purpose:
- Classify employment type from title/description text.

Pipeline behavior:
1. Keyword pattern matching for: full_time, part_time, internship,
   contractual, temporary, freelance
2. Rule confidence scoring
3. Gemini fallback when no confident rule-based class is found

Main outputs:
- job_type
- confidence_score
- method_used

Usage:
- `JobTypeExtractionModule().transform(record)` for pipeline usage.
- `extract_job_type(title, description)` for one-off usage.

Notes:
- Uses Gemini fallback only when rule matching finds no confident label.
- If fallback is unavailable, returns `job_type=not_specified`.
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
class JobTypeExtractionConfig:
    """Configuration for job type extraction."""

    title_key: str = "clean_title"
    description_key: str = "clean_description"
    output_label_key: str = "job_type"
    output_confidence_key: str = "confidence_score"
    output_method_key: str = "method_used"
    default_method_name: str = "rule"
    fallback_method_name: str = "gemini"
    fallback_confidence: str = "gemini"
    allowed_types: Tuple[str, ...] = (
        "full_time",
        "part_time",
        "internship",
        "contractual",
        "temporary",
        "freelance",
    )


class JobTypeExtractionModule:
    """Class-based extractor for employment type with fallback support."""

    _patterns: Dict[str, List[re.Pattern]] = {
        "full_time": [
            re.compile(r"\bfull\s*[- ]?time\b", re.IGNORECASE),
            re.compile(r"\bpermanent\b", re.IGNORECASE),
            re.compile(r"\bregular\s*position\b", re.IGNORECASE),
        ],
        "part_time": [
            re.compile(r"\bpart\s*[- ]?time\b", re.IGNORECASE),
            re.compile(r"\bpart\s*[- ]?timer\b", re.IGNORECASE),
        ],
        "internship": [
            re.compile(r"\binternship\b", re.IGNORECASE),
            re.compile(r"\bintern\b", re.IGNORECASE),
            re.compile(r"\btrainee\b", re.IGNORECASE),
            re.compile(r"\bgraduate\s*program\b", re.IGNORECASE),
        ],
        "contractual": [
            re.compile(r"\bcontract\b", re.IGNORECASE),
            re.compile(r"\bfixed\s*term\b", re.IGNORECASE),
            re.compile(r"\bconsultant\b", re.IGNORECASE),
        ],
        "temporary": [
            re.compile(r"\btemporary\b", re.IGNORECASE),
            re.compile(r"\btemp\b", re.IGNORECASE),
            re.compile(r"\bshort\s*term\b", re.IGNORECASE),
            re.compile(r"\bseasonal\b", re.IGNORECASE),
        ],
        "freelance": [
            re.compile(r"\bfreelance\b", re.IGNORECASE),
            re.compile(r"\bfreelancer\b", re.IGNORECASE),
            re.compile(r"\bindependent\s*contractor\b", re.IGNORECASE),
        ],
    }

    def __init__(
        self,
        config: Optional[JobTypeExtractionConfig] = None,
        gemini_callable: Optional[Callable[[str], str]] = None,
    ):
        self.config = config or JobTypeExtractionConfig()
        self._gemini_callable = gemini_callable
        logger.debug("JobTypeExtractionModule initialized: %s", self.config)

    def _build_text(self, title: Optional[str], description: Optional[str]) -> str:
        return f"{title or ''}\n{description or ''}".strip()

    def _score(self, text: str) -> Dict[str, int]:
        scores = {label: 0 for label in self.config.allowed_types}
        for label, pats in self._patterns.items():
            if label not in scores:
                continue
            for pat in pats:
                if pat.search(text):
                    scores[label] += 1
        return scores

    def _rule_extract(self, text: str) -> Optional[Tuple[str, float]]:
        if not text:
            return None

        scores = self._score(text)
        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        top_label, top_score = ranked[0]
        second_score = ranked[1][1] if len(ranked) > 1 else 0

        if top_score == 0:
            return None

        # Prefer internship if explicitly present; avoids tie issues with full-time text.
        if scores.get("internship", 0) > 0:
            return "internship", 0.9

        margin = max(top_score - second_score, 0)
        confidence = min(0.62 + 0.12 * margin + 0.05 * top_score, 0.95)
        return top_label, round(confidence, 2)

    def _build_gemini_prompt(self, text: str) -> str:
        choices = ", ".join(self.config.allowed_types)
        return (
            f"Determine job type: {choices}.\n\n"
            "Return only one.\n\n"
            f"Job description:\n{text}"
        )

    def _normalize_gemini_label(self, raw: str) -> Optional[str]:
        value = (raw or "").strip().lower()
        if not value:
            return None

        mappings = {
            "full time": "full_time",
            "full-time": "full_time",
            "part time": "part_time",
            "part-time": "part_time",
            "internship": "internship",
            "intern": "internship",
            "contract": "contractual",
            "contractual": "contractual",
            "temporary": "temporary",
            "temp": "temporary",
            "freelance": "freelance",
            "freelancer": "freelance",
        }

        if value in mappings:
            return mappings[value]

        for key, normalized in mappings.items():
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
        """Extract job type using rule-first and Gemini fallback."""
        text = self._build_text(title, description)
        logger.debug("JobTypeExtraction.extract text_len=%d", len(text))

        rule = self._rule_extract(text)
        if rule is not None:
            label, confidence = rule
            logger.info("JobTypeExtraction.rule match label=%s confidence=%s", label, confidence)
            return {
                self.config.output_label_key: label,
                self.config.output_confidence_key: confidence,
                self.config.output_method_key: self.config.default_method_name,
            }

        prompt = self._build_gemini_prompt(text)
        logger.debug("JobTypeExtraction calling Gemini fallback")
        raw = self._call_gemini(prompt)
        label = self._normalize_gemini_label(raw or "")
        if label is not None:
            logger.info("JobTypeExtraction.gemini fallback label=%s", label)
            return {
                self.config.output_label_key: label,
                self.config.output_confidence_key: self.config.fallback_confidence,
                self.config.output_method_key: self.config.fallback_method_name,
            }

        return {
            self.config.output_label_key: "not_specified",
            self.config.output_confidence_key: 0.0,
            self.config.output_method_key: f"{self.config.default_method_name}_fallback_unavailable",
        }

    def transform(self, record: Dict[str, str]) -> Dict[str, object]:
        """Return record copy with extracted job-type fields attached."""
        output: Dict[str, object] = dict(record)
        output.update(
            self.extract(
                title=record.get(self.config.title_key),
                description=record.get(self.config.description_key),
            )
        )
        return output


def extract_job_type(
    title: Optional[str],
    description: Optional[str],
    config: Optional[JobTypeExtractionConfig] = None,
    gemini_callable: Optional[Callable[[str], str]] = None,
) -> Dict[str, object]:
    """Convenience function for one-off job type extraction."""
    module = JobTypeExtractionModule(config=config, gemini_callable=gemini_callable)
    return module.extract(title=title, description=description)


__all__ = [
    "JobTypeExtractionConfig",
    "JobTypeExtractionModule",
    "extract_job_type",
]
