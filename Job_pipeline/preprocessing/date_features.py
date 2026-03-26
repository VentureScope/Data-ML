"""Step 3: Date features module.

Purpose:
- Convert raw posting timestamps into structured, modeling-ready features.

Pipeline behavior:
1. Parse date from common input formats (ISO, slash/dash, compact digits)
2. Emit canonical timestamp
3. Derive year/month/year_month/week/quarter
4. Compute holiday_flag via the `holidays` package (configurable by country)

Main outputs:
- timestamp
- year, month, year_month, week, quarter
- holiday_flag

Usage:
- Use `DateFeaturesModule().transform(record)` for record-level enrichment.
- Use `extract_date_features(raw_date)` for one-off extraction.

Notes:
- This module has no Gemini fallback.
- Built-in holiday lookup can be disabled via `use_builtin_holidays=False`.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, Optional, Set

import holidays


logger = logging.getLogger(__name__)


@dataclass
class DateFeaturesConfig:
    """Configuration for date parsing and feature extraction."""

    date_key: str = "created_at"
    output_prefix: str = ""
    output_timestamp_key: str = "timestamp"
    country_code: Optional[str] = "ET"
    subdivision: Optional[str] = None
    use_builtin_holidays: bool = True
    holiday_dates: Set[str] = field(default_factory=set)


class DateFeaturesModule:
    """Class-based extractor for job posting date features."""

    _non_digits_re = re.compile(r"\D")

    def __init__(self, config: Optional[DateFeaturesConfig] = None):
        self.config = config or DateFeaturesConfig()
        self._holiday_cache = None

    def _prefixed(self, key: str) -> str:
        return f"{self.config.output_prefix}{key}"

    def _parse_datetime(self, raw_value: Optional[str]) -> Optional[datetime]:
        raw = (raw_value or "").strip()
        if not raw:
            return None

        # First pass: ISO-style inputs, including timezone suffixes.
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            pass

        formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M",
            "%Y-%m",
            "%Y/%m",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue

        # Compact numeric fallbacks like YYYYMMDD or YYYYMM.
        digits = self._non_digits_re.sub("", raw)
        if len(digits) >= 8:
            try:
                return datetime.strptime(digits[:8], "%Y%m%d")
            except ValueError:
                pass
        if len(digits) >= 6:
            try:
                return datetime.strptime(digits[:6], "%Y%m")
            except ValueError:
                pass

        return None

    def _load_builtin_holidays(self):
        if not self.config.use_builtin_holidays or not self.config.country_code:
            return None

        if self._holiday_cache is not None:
            return self._holiday_cache

        kwargs = {}
        if self.config.subdivision:
            kwargs["subdiv"] = self.config.subdivision
        self._holiday_cache = holidays.country_holidays(self.config.country_code, **kwargs)
        return self._holiday_cache

    def _is_holiday(self, dt_date: date) -> bool:
        if dt_date.strftime("%Y-%m-%d") in self.config.holiday_dates:
            return True

        holiday_calendar = self._load_builtin_holidays()
        if holiday_calendar is None:
            return False

        return dt_date in holiday_calendar

    def transform_date(self, raw_date: Optional[str]) -> Dict[str, Optional[object]]:
        """Extract date features from a date-like string."""
        logger.debug("transform_date input=%s", raw_date)
        dt = self._parse_datetime(raw_date)
        if dt is None:
            return {
                self._prefixed(self.config.output_timestamp_key): None,
                self._prefixed("year"): None,
                self._prefixed("month"): None,
                self._prefixed("year_month"): None,
                self._prefixed("week"): None,
                self._prefixed("quarter"): None,
                self._prefixed("holiday_flag"): False,
            }

        iso_week = dt.isocalendar().week
        quarter = ((dt.month - 1) // 3) + 1

        return {
            self._prefixed(self.config.output_timestamp_key): dt.isoformat(),
            self._prefixed("year"): dt.year,
            self._prefixed("month"): dt.month,
            self._prefixed("year_month"): f"{dt.year:04d}-{dt.month:02d}",
            self._prefixed("week"): int(iso_week),
            self._prefixed("quarter"): quarter,
            self._prefixed("holiday_flag"): self._is_holiday(dt.date()),
        }

    def transform(self, record: Dict[str, str]) -> Dict[str, Optional[object]]:
        """Return a copy of the input record with extracted date features."""
        output = dict(record)
        output.update(self.transform_date(record.get(self.config.date_key)))
        return output


def extract_date_features(
    raw_date: Optional[str],
    config: Optional[DateFeaturesConfig] = None,
) -> Dict[str, Optional[object]]:
    """Convenience function for one-off date feature extraction."""
    module = DateFeaturesModule(config=config)
    return module.transform_date(raw_date)


__all__ = ["DateFeaturesConfig", "DateFeaturesModule", "extract_date_features"]
