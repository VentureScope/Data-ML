"""Step 7: Remote detection module.

Purpose:
- Detect work arrangement signals and expose both mode and boolean remote flag.

Pipeline behavior:
1. Keyword scoring for remote/hybrid/onsite
2. Rule decision with confidence
3. Gemini fallback when no keyword evidence exists

Main outputs:
- is_remote (bool)
- remote_mode (remote/hybrid/onsite/unknown)
- confidence_score
- method_used

Usage:
- `RemoteDetectionModule().transform(record)` for record-level usage.
- `detect_remote_mode(title, description)` for one-off usage.

Notes:
- Uses Gemini fallback only when rule evidence is absent.
- If fallback is unavailable, returns `remote_mode=unknown`.
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
class RemoteDetectionConfig:
    """Configuration for remote/hybrid/onsite detection."""

    description_key: str = "clean_description"
    title_key: str = "clean_title"
    output_label_key: str = "is_remote"
    output_mode_key: str = "remote_mode"
    output_confidence_key: str = "confidence_score"
    output_method_key: str = "method_used"
    default_method_name: str = "rule"
    fallback_method_name: str = "gemini"
    fallback_confidence: str = "gemini"


class RemoteDetectionModule:
    """Detect work mode with keyword rules and Gemini fallback."""

    _keyword_patterns: Dict[str, List[re.Pattern]] = {
        "remote": [
            re.compile(r"\bremote\b", re.IGNORECASE),
            re.compile(r"\bwork\s*from\s*home\b", re.IGNORECASE),
            re.compile(r"\bwfh\b", re.IGNORECASE),
            re.compile(r"\banywhere\b", re.IGNORECASE),
            re.compile(r"\bfully\s*remote\b", re.IGNORECASE),
            re.compile(r"\bremote\s*friendly\b", re.IGNORECASE),
        ],
        "hybrid": [
            re.compile(r"\bhybrid\b", re.IGNORECASE),
            re.compile(r"\bpartly\s*remote\b", re.IGNORECASE),
            re.compile(r"\bremote\s*and\s*onsite\b", re.IGNORECASE),
            re.compile(r"\b[0-9]+\s*days?\s*(on\s*site|onsite|in\s*office)\b", re.IGNORECASE),
        ],
        "onsite": [
            re.compile(r"\bonsite\b", re.IGNORECASE),
            re.compile(r"\bon-site\b", re.IGNORECASE),
            re.compile(r"\bin\s*office\b", re.IGNORECASE),
            re.compile(r"\bon\s*premi[sz]e\b", re.IGNORECASE),
            re.compile(r"\bat\s*our\s*office\b", re.IGNORECASE),
        ],
    }

    def __init__(
        self,
        config: Optional[RemoteDetectionConfig] = None,
        gemini_callable: Optional[Callable[[str], str]] = None,
    ):
        self.config = config or RemoteDetectionConfig()
        self._gemini_callable = gemini_callable
        logger.debug("RemoteDetectionModule initialized: %s", self.config)

    def _build_text(self, title: Optional[str], description: Optional[str]) -> str:
        return f"{title or ''}\n{description or ''}".strip()

    def _score_keywords(self, text: str) -> Dict[str, int]:
        scores = {"remote": 0, "hybrid": 0, "onsite": 0}
        for label, patterns in self._keyword_patterns.items():
            for pattern in patterns:
                if pattern.search(text):
                    scores[label] += 1
        return scores

    def _rule_detect(self, text: str) -> Optional[Tuple[str, float]]:
        if not text:
            return None

        scores = self._score_keywords(text)
        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        top_label, top_score = ranked[0]
        second_score = ranked[1][1]

        if top_score == 0:
            return None

        # If signals conflict strongly, return hybrid when present.
        if scores["hybrid"] > 0 and (scores["remote"] > 0 or scores["onsite"] > 0):
            return "hybrid", 0.72

        # Soft confidence based on margin between top and second scores.
        margin = max(top_score - second_score, 0)
        confidence = min(0.6 + 0.15 * margin + 0.05 * top_score, 0.95)
        return top_label, round(confidence, 2)

    def _build_gemini_prompt(self, text: str) -> str:
        return (
            "Classify this job as:\n"
            "remote, hybrid, or onsite.\n\n"
            "Return only one word.\n\n"
            f"Job description:\n{text}"
        )

    def _normalize_gemini_label(self, raw: str) -> Optional[str]:
        value = (raw or "").strip().lower()
        if not value:
            return None

        if "hybrid" in value:
            return "hybrid"
        if "remote" in value:
            return "remote"
        if "onsite" in value or "on-site" in value or "on site" in value:
            return "onsite"
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

    def detect(self, title: Optional[str], description: Optional[str]) -> Dict[str, object]:
        """Detect work mode with rule-first then Gemini fallback."""
        text = self._build_text(title, description)
        logger.debug("RemoteDetection.detect text_len=%d", len(text))

        rule = self._rule_detect(text)
        if rule is not None:
            label, confidence = rule
            logger.info("RemoteDetection.rule match label=%s confidence=%s", label, confidence)
            return {
                self.config.output_label_key: label == "remote",
                self.config.output_mode_key: label,
                self.config.output_confidence_key: confidence,
                self.config.output_method_key: self.config.default_method_name,
            }

        prompt = self._build_gemini_prompt(text)
        logger.debug("RemoteDetection calling Gemini fallback")
        gemini_raw = self._call_gemini(prompt)
        label = self._normalize_gemini_label(gemini_raw or "")
        if label is not None:
            logger.info("RemoteDetection.gemini fallback label=%s", label)
            return {
                self.config.output_label_key: label == "remote",
                self.config.output_mode_key: label,
                self.config.output_confidence_key: self.config.fallback_confidence,
                self.config.output_method_key: self.config.fallback_method_name,
            }

        return {
            self.config.output_label_key: False,
            self.config.output_mode_key: "unknown",
            self.config.output_confidence_key: 0.0,
            self.config.output_method_key: f"{self.config.default_method_name}_fallback_unavailable",
        }

    def transform(self, record: Dict[str, str]) -> Dict[str, object]:
        """Return record copy with work-mode fields attached."""
        output: Dict[str, object] = dict(record)
        output.update(
            self.detect(
                title=record.get(self.config.title_key),
                description=record.get(self.config.description_key),
            )
        )
        return output


def detect_remote_mode(
    title: Optional[str],
    description: Optional[str],
    config: Optional[RemoteDetectionConfig] = None,
    gemini_callable: Optional[Callable[[str], str]] = None,
) -> Dict[str, object]:
    """Convenience function for one-off remote mode detection."""
    module = RemoteDetectionModule(config=config, gemini_callable=gemini_callable)
    return module.detect(title=title, description=description)


__all__ = [
    "RemoteDetectionConfig",
    "RemoteDetectionModule",
    "detect_remote_mode",
]
