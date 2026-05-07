"""Step 2: Job ID generation module.

Purpose:
- Generate a deterministic, stable identifier for deduplication and tracking.

Pipeline behavior:
1. Normalize text fields (title/company/source)
2. Preserve raw date string by default; set date_granularity="month" to
   truncate to YYYY-MM (reduces precision but groups same-month duplicates)
3. Build canonical signature: title|company|date|source
4. Hash signature (sha256 by default) and truncate to configured length

Main output:
- job_id

Usage:
- Configure key mappings with `JobIdConfig`.
- Call `JobIdModule(...).transform(record)`.
- Or call `generate_job_id(...)` for one-off usage.

Notes:
- This module is deterministic and has no Gemini/LLM fallback.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional


@dataclass
class JobIdConfig:
    """Configuration for job ID generation."""

    title_key: str = "title"
    company_key: str = "company"
    date_key: str = "date"
    source_key: str = "source"
    output_key: str = "job_id"
    date_granularity: str = "raw"  # supported: month, raw
    hash_algorithm: str = "sha256"
    hash_length: int = 16


class JobIdModule:
    """Class-based generator for deterministic job IDs."""

    _space_re = re.compile(r"\s+")

    def __init__(self, config: Optional[JobIdConfig] = None):
        self.config = config or JobIdConfig()
        self._validate_config()
        self.logger = logging.getLogger(__name__)
        self.logger.debug("JobIdModule initialized: %s", self.config)

    def _validate_config(self) -> None:
        if self.config.date_granularity not in {"month", "raw"}:
            raise ValueError("date_granularity must be either 'month' or 'raw'")
        if self.config.hash_length <= 0:
            raise ValueError("hash_length must be a positive integer")

    def _normalize_text(self, value: Optional[str]) -> str:
        text = (value or "").strip().lower()
        return self._space_re.sub(" ", text)

    def _to_month(self, date_value: Optional[str]) -> str:
        """Convert a date-like value into YYYY-MM when possible."""
        raw = (date_value or "").strip()
        if not raw:
            return ""

        candidates = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%Y-%m",
            "%Y/%m",
        ]

        # Attempt ISO parsing first.
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m")
        except ValueError:
            pass

        for fmt in candidates:
            try:
                dt = datetime.strptime(raw, fmt)
                return dt.strftime("%Y-%m")
            except ValueError:
                continue

        # Handle compact forms like YYYYMMDD or YYYYMM.
        digits = re.sub(r"\D", "", raw)
        if len(digits) >= 6 and digits[:6].isdigit():
            year = digits[:4]
            month = digits[4:6]
            if 1 <= int(month) <= 12:
                return f"{year}-{month}"

        return ""

    def _normalize_date(self, date_value: Optional[str]) -> str:
        if self.config.date_granularity == "month":
            return self._to_month(date_value)
        return self._normalize_text(date_value)

    def build_signature(
        self,
        title: Optional[str],
        company: Optional[str],
        date_value: Optional[str],
        source: Optional[str],
    ) -> str:
        """Build canonical signature before hashing."""
        parts = [
            self._normalize_text(title),
            self._normalize_text(company),
            self._normalize_date(date_value),
            self._normalize_text(source),
        ]
        return "|".join(parts)

    def generate_id(
        self,
        title: Optional[str],
        company: Optional[str],
        date_value: Optional[str],
        source: Optional[str],
    ) -> str:
        """Generate deterministic hash ID from core identity fields."""
        signature = self.build_signature(title, company, date_value, source)
        self.logger.debug("Generating job_id signature=%s", signature)
        digest = hashlib.new(self.config.hash_algorithm, signature.encode("utf-8")).hexdigest()
        return digest[: self.config.hash_length]

    def transform(self, record: Dict[str, str]) -> Dict[str, str]:
        """Return a copy of record with generated job ID attached."""
        output = dict(record)
        output[self.config.output_key] = self.generate_id(
            title=record.get(self.config.title_key),
            company=record.get(self.config.company_key),
            date_value=record.get(self.config.date_key),
            source=record.get(self.config.source_key),
        )
        self.logger.info("Generated job_id=%s for title=%s", output[self.config.output_key], record.get(self.config.title_key))
        return output


def generate_job_id(
    title: Optional[str],
    company: Optional[str],
    date_value: Optional[str],
    source: Optional[str],
    config: Optional[JobIdConfig] = None,
) -> str:
    """Convenience function for one-off ID generation."""
    module = JobIdModule(config=config)
    return module.generate_id(
        title=title,
        company=company,
        date_value=date_value,
        source=source,
    )


__all__ = ["JobIdConfig", "JobIdModule", "generate_job_id"]
