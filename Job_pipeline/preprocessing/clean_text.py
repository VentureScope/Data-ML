"""Step 1: Text cleaning module.

Purpose:
- Normalize raw title/description text before any downstream extraction.

Pipeline behavior:
1. Unicode normalization
2. HTML decoding + tag removal
3. Emoji removal
4. Lowercasing
5. Boilerplate phrase stripping
6. Punctuation removal
7. Whitespace normalization

Main outputs:
- clean_title
- clean_description

Usage:
- Create `CleanTextModule()` and call `transform(title, description)`.
- Or call `clean_job_text(title, description)` for one-off usage.

Notes:
- This module is deterministic and has no Gemini/LLM fallback.
"""

from __future__ import annotations

import html
import re
import unicodedata
import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional


logger = logging.getLogger(__name__)


@dataclass
class TextCleanerConfig:
	"""Configuration flags and patterns for text cleaning."""

	lowercase: bool = True
	remove_html: bool = True
	remove_punctuation: bool = True
	remove_extra_whitespace: bool = True
	remove_emojis: bool = True
	normalize_unicode: bool = True
	remove_boilerplate: bool = True
	boilerplate_patterns: Iterable[str] = field(
		default_factory=lambda: (
			r"apply\s+now",
			r"click\s+here\s+to\s+apply",
			r"send\s+(your\s+)?(cv|resume)",
			r"equal\s+opportunity\s+employer",
			r"we\s+are\s+hiring",
			r"job\s+summary",
			r"job\s+description",
			r"about\s+the\s+company",
			r"about\s+us",
			r"read\s+more",
		)
	)


class CleanTextModule:
	"""Class-based cleaner for job title and description fields."""

	_html_tag_re = re.compile(r"<[^>]+>")
	_punct_re = re.compile(r"[^\w\s]")
	_whitespace_re = re.compile(r"\s+")
	_emoji_re = re.compile(
		"["
		"\U0001F300-\U0001F5FF"  # symbols & pictographs
		"\U0001F600-\U0001F64F"  # emoticons
		"\U0001F680-\U0001F6FF"  # transport & map
		"\U0001F700-\U0001F77F"  # alchemical
		"\U0001F780-\U0001F7FF"  # geometric extended
		"\U0001F800-\U0001F8FF"  # supplemental arrows-c
		"\U0001F900-\U0001F9FF"  # supplemental symbols
		"\U0001FA00-\U0001FAFF"  # symbols and pictographs ext-a
		"\U00002700-\U000027BF"  # dingbats
		"\U00002600-\U000026FF"  # misc symbols
		"]+",
		flags=re.UNICODE,
	)

	def __init__(self, config: Optional[TextCleanerConfig] = None):
		self.config = config or TextCleanerConfig()
		self._boilerplate_res = [
			re.compile(pattern, flags=re.IGNORECASE)
			for pattern in self.config.boilerplate_patterns
		]
		logger.debug("CleanTextModule initialized: %s", self.config)

	def clean_text(self, text: Optional[str]) -> str:
		"""Clean a single text input and return normalized text."""
		value = text or ""
		logger.debug("clean_text start len=%d", len(value))

		if self.config.normalize_unicode:
			value = unicodedata.normalize("NFKC", value)

		if self.config.remove_html:
			value = html.unescape(value)
			value = self._html_tag_re.sub(" ", value)

		if self.config.remove_emojis:
			value = self._emoji_re.sub(" ", value)

		if self.config.lowercase:
			value = value.lower()

		if self.config.remove_boilerplate:
			for boilerplate_re in self._boilerplate_res:
				value = boilerplate_re.sub(" ", value)

		if self.config.remove_punctuation:
			value = self._punct_re.sub(" ", value)

		if self.config.remove_extra_whitespace:
			value = self._whitespace_re.sub(" ", value).strip()

		logger.debug("clean_text end len=%d", len(value))
		return value

	def transform(self, title: Optional[str], description: Optional[str]) -> Dict[str, str]:
		"""Transform title and description into the Step 1 output schema."""
		result = {
			"clean_title": self.clean_text(title),
			"clean_description": self.clean_text(description),
		}
		logger.info(
			"Step1.clean_text complete: clean_title_len=%d clean_description_len=%d",
			len(result["clean_title"]),
			len(result["clean_description"]),
		)
		return result

	def transform_record(
		self,
		record: Dict[str, str],
		title_key: str = "title",
		description_key: str = "description",
	) -> Dict[str, str]:
		"""Clean fields from a record dict and merge outputs into a new dict."""
		output = dict(record)
		output.update(
			self.transform(
				title=record.get(title_key),
				description=record.get(description_key),
			)
		)
		return output


def clean_job_text(
	title: Optional[str],
	description: Optional[str],
	config: Optional[TextCleanerConfig] = None,
) -> Dict[str, str]:
	"""Convenience function for one-off cleaning without class instantiation."""
	cleaner = CleanTextModule(config=config)
	return cleaner.transform(title=title, description=description)


__all__ = ["TextCleanerConfig", "CleanTextModule", "clean_job_text"]
