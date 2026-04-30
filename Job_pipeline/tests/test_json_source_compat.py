"""Tests for JSON data source compatibility.

Tests the hint normalization, field resolution, and filtering logic
added for ``jobs_has_link.json`` and ``no_link_jobs.json`` without
requiring heavy ML dependencies (sentence-transformers, torch).

We mock out the unavailable modules at the top before any pipeline
imports touch them.
"""

from __future__ import annotations

import sys
import unittest
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Mock heavy ML modules that may not be installed locally.
# ---------------------------------------------------------------------------
for _mod_name in (
    "sentence_transformers",
    "torch",
    "spacy",
    "rapidfuzz",
    "rapidfuzz.fuzz",
    "google",
    "google.genai",
):
    if _mod_name not in sys.modules:
        sys.modules[_mod_name] = MagicMock()

# Now safe to import pipeline modules.
from Job_pipeline.preprocessing.job_type_extraction import JobTypeExtractionModule
from Job_pipeline.preprocessing.remote_detection import RemoteDetectionModule
from Job_pipeline.preprocessing.education_extraction import EducationExtractionModule
from Job_pipeline.preprocessing.unified_preprocessor import (
    _resolve_key,
    _FIELD_ALIASES,
    UnifiedPreprocessor,
)


# ===================================================================
# 1) Job-Type Hint Tests
# ===================================================================

class TestJobTypeHintNormalization(unittest.TestCase):
    """Test the _normalize_hint method on JobTypeExtractionModule."""

    def setUp(self) -> None:
        self.mod = JobTypeExtractionModule(gemini_callable=lambda _: None)

    def test_uppercase_enum_values(self) -> None:
        """FULL_TIME, PART_TIME etc from jobs_has_link.json."""
        self.assertEqual(self.mod._normalize_hint("FULL_TIME"), "full_time")
        self.assertEqual(self.mod._normalize_hint("PART_TIME"), "part_time")
        self.assertEqual(self.mod._normalize_hint("FREELANCE"), "freelance")

    def test_compound_string_full_time(self) -> None:
        """'On-site - Permanent (Full-time)' from no_link_jobs.json."""
        self.assertEqual(
            self.mod._normalize_hint("On-site - Permanent (Full-time)"),
            "full_time",
        )

    def test_compound_string_contractual(self) -> None:
        """'Remote - Contractual' from no_link_jobs.json."""
        self.assertEqual(
            self.mod._normalize_hint("Remote - Contractual"),
            "contractual",
        )

    def test_none_and_empty(self) -> None:
        self.assertIsNone(self.mod._normalize_hint(None))
        self.assertIsNone(self.mod._normalize_hint(""))
        self.assertIsNone(self.mod._normalize_hint("  "))

    def test_hint_used_in_extract(self) -> None:
        """extract() should use hint with high confidence before text rules."""
        result = self.mod.extract(
            title="Accountant",
            description="Handle financial records",
            hint="FULL_TIME",
        )
        self.assertEqual(result["job_type"], "full_time")
        self.assertEqual(result["method_used"], "structured_hint")
        self.assertEqual(result["confidence_score"], 0.95)

    def test_no_hint_falls_through_to_rules(self) -> None:
        """Without hint, extract() should use rules/text as before."""
        result = self.mod.extract(
            title="Freelance Designer",
            description="We need a freelance graphic designer",
            hint=None,
        )
        self.assertEqual(result["job_type"], "freelance")
        self.assertNotEqual(result["method_used"], "structured_hint")


# ===================================================================
# 2) Remote Detection Hint Tests
# ===================================================================

class TestRemoteDetectionHintNormalization(unittest.TestCase):
    """Test the _normalize_hint method on RemoteDetectionModule."""

    def setUp(self) -> None:
        self.mod = RemoteDetectionModule(gemini_callable=lambda _: None)

    def test_uppercase_enum_values(self) -> None:
        """REMOTE, ONSITE, HYBRID from jobs_has_link.json."""
        self.assertEqual(self.mod._normalize_hint("REMOTE"), "remote")
        self.assertEqual(self.mod._normalize_hint("ONSITE"), "onsite")
        self.assertEqual(self.mod._normalize_hint("HYBRID"), "hybrid")

    def test_compound_remote(self) -> None:
        """'Remote - Contractual' should yield 'remote'."""
        self.assertEqual(
            self.mod._normalize_hint("Remote - Contractual"),
            "remote",
        )

    def test_compound_onsite(self) -> None:
        """'On-site - Permanent (Full-time)' should yield 'onsite'."""
        self.assertEqual(
            self.mod._normalize_hint("On-site - Permanent (Full-time)"),
            "onsite",
        )

    def test_none_and_empty(self) -> None:
        self.assertIsNone(self.mod._normalize_hint(None))
        self.assertIsNone(self.mod._normalize_hint(""))

    def test_hint_used_in_detect(self) -> None:
        """detect() should use hint with high confidence."""
        result = self.mod.detect(
            title="Backend Dev",
            description="Build APIs",
            hint="REMOTE",
        )
        self.assertTrue(result["is_remote"])
        self.assertEqual(result["remote_mode"], "remote")
        self.assertEqual(result["method_used"], "structured_hint")

    def test_onsite_hint(self) -> None:
        """ONSITE hint should yield is_remote=False."""
        result = self.mod.detect(
            title="Backend Dev",
            description="Build APIs",
            hint="ONSITE",
        )
        self.assertFalse(result["is_remote"])
        self.assertEqual(result["remote_mode"], "onsite")


# ===================================================================
# 3) Education Hint Tests
# ===================================================================

class TestEducationHintNormalization(unittest.TestCase):
    """Test the _normalize_hint method on EducationExtractionModule."""

    def setUp(self) -> None:
        self.mod = EducationExtractionModule(gemini_callable=lambda _: None)

    def test_structured_enum_values(self) -> None:
        """Standard enum values from jobs_has_link.json."""
        self.assertEqual(self.mod._normalize_hint("BACHELORS_DEGREE"), "Bachelors")
        self.assertEqual(self.mod._normalize_hint("MASTERS_DEGREE"), "Masters")
        self.assertEqual(self.mod._normalize_hint("DIPLOMA"), "Diploma")
        self.assertEqual(self.mod._normalize_hint("TVET"), "Diploma")
        self.assertEqual(self.mod._normalize_hint("CERTIFICATE"), "Diploma")

    def test_not_required_returns_none(self) -> None:
        """NOT_REQUIRED should return None (skip hint, let text try)."""
        self.assertIsNone(self.mod._normalize_hint("NOT_REQUIRED"))

    def test_none_and_empty(self) -> None:
        self.assertIsNone(self.mod._normalize_hint(None))
        self.assertIsNone(self.mod._normalize_hint(""))

    def test_hint_used_in_extract(self) -> None:
        """extract() should use hint with high confidence."""
        result = self.mod.extract(
            title="Accountant",
            description="Handle financial records",
            hint="BACHELORS_DEGREE",
        )
        self.assertEqual(result["education_level"], "Bachelors")
        self.assertEqual(result["method_used"], "structured_hint")

    def test_not_required_falls_through(self) -> None:
        """NOT_REQUIRED hint should fall through to text extraction."""
        result = self.mod.extract(
            title="Software Developer",
            description="Must have a Bachelor's degree in CS",
            hint="NOT_REQUIRED",
        )
        self.assertEqual(result["education_level"], "Bachelors")
        self.assertNotEqual(result["method_used"], "structured_hint")


# ===================================================================
# 4) Field Alias Resolution Tests
# ===================================================================

class TestFieldAliasResolution(unittest.TestCase):
    """Test _resolve_key with the expanded alias map."""

    def test_canonical_key_returned_first(self) -> None:
        row = {"created_at": "2026-01-01", "posted_date": "2026-02-02"}
        self.assertEqual(_resolve_key(row, "created_at"), "2026-01-01")

    def test_alias_fallback_posted_date(self) -> None:
        row = {"posted_date": "2026-04-27T14:37:20+00:00"}
        self.assertEqual(
            _resolve_key(row, "created_at"),
            "2026-04-27T14:37:20+00:00",
        )

    def test_alias_fallback_date(self) -> None:
        row = {"date": "2026-04-24 10:47:55"}
        self.assertEqual(_resolve_key(row, "created_at"), "2026-04-24 10:47:55")

    def test_alias_job_title(self) -> None:
        row = {"job_title": "Data Analyst"}
        self.assertEqual(_resolve_key(row, "title"), "Data Analyst")

    def test_alias_company_name(self) -> None:
        row = {"company_name": "Minaye PLC"}
        self.assertEqual(_resolve_key(row, "entity_name"), "Minaye PLC")

    def test_alias_company(self) -> None:
        row = {"company": "POSSIBLE TECHNOLOGY PLC"}
        self.assertEqual(_resolve_key(row, "entity_name"), "POSSIBLE TECHNOLOGY PLC")

    def test_alias_work_location(self) -> None:
        row = {"work_location": "Addis Ababa, Ethiopia"}
        self.assertEqual(_resolve_key(row, "location"), "Addis Ababa, Ethiopia")

    def test_alias_job_type_hint(self) -> None:
        row = {"job_type": "FULL_TIME"}
        self.assertEqual(_resolve_key(row, "job_type_hint"), "FULL_TIME")

    def test_alias_job_site_hint(self) -> None:
        row = {"job_site": "REMOTE"}
        self.assertEqual(_resolve_key(row, "job_site_hint"), "REMOTE")

    def test_alias_skills_hint(self) -> None:
        row = {"skills": ["Python", "SQL"]}
        self.assertEqual(_resolve_key(row, "skills_hint"), ["Python", "SQL"])

    def test_alias_education_hint(self) -> None:
        row = {"education_qualification": "BACHELORS_DEGREE"}
        self.assertEqual(
            _resolve_key(row, "education_hint"), "BACHELORS_DEGREE"
        )

    def test_missing_returns_none(self) -> None:
        row = {"foo": "bar"}
        self.assertIsNone(_resolve_key(row, "created_at"))

    def test_scraped_at_as_date_fallback(self) -> None:
        """scraped_at should be a last-resort alias for created_at."""
        row = {"scraped_at": "2026-04-28T10:22:31.260Z"}
        self.assertEqual(_resolve_key(row, "created_at"), "2026-04-28T10:22:31.260Z")

    def test_all_aliases_have_valid_structure(self) -> None:
        """Ensure _FIELD_ALIASES values are all lists of strings."""
        for canonical, aliases in _FIELD_ALIASES.items():
            self.assertIsInstance(aliases, list, f"{canonical} aliases not a list")
            for alias in aliases:
                self.assertIsInstance(alias, str, f"{canonical} has non-str alias: {alias}")


# ===================================================================
# 5) Best Description (raw_text fallback) Tests
# ===================================================================

class TestBestDescription(unittest.TestCase):
    """Test the _best_description static method."""

    def test_raw_text_preferred_when_longer(self) -> None:
        working = {
            "description": "Short truncated text",
            "raw_text": "This is the full raw text with much more detail about the job posting",
        }
        UnifiedPreprocessor._best_description(working, "description")
        self.assertEqual(
            working["description"],
            "This is the full raw text with much more detail about the job posting",
        )

    def test_description_kept_when_longer(self) -> None:
        working = {
            "description": "This is a very detailed and complete job description with all requirements listed",
            "raw_text": "Short raw text",
        }
        UnifiedPreprocessor._best_description(working, "description")
        self.assertEqual(
            working["description"],
            "This is a very detailed and complete job description with all requirements listed",
        )

    def test_no_raw_text(self) -> None:
        working = {"description": "Normal description"}
        UnifiedPreprocessor._best_description(working, "description")
        self.assertEqual(working["description"], "Normal description")

    def test_none_raw_text(self) -> None:
        working = {"description": "Normal description", "raw_text": None}
        UnifiedPreprocessor._best_description(working, "description")
        self.assertEqual(working["description"], "Normal description")

    def test_both_empty(self) -> None:
        working = {"description": "", "raw_text": ""}
        UnifiedPreprocessor._best_description(working, "description")
        self.assertEqual(working["description"], "")


# ===================================================================
# 6) Build Location Value Tests
# ===================================================================

class TestBuildLocationValue(unittest.TestCase):
    """Test the _build_location_value method."""

    def setUp(self) -> None:
        # Use __new__ to bypass __init__ which loads heavy ML models.
        self.pp = UnifiedPreprocessor.__new__(UnifiedPreprocessor)

    def test_explicit_location_field(self) -> None:
        row = {"location": "Addis Ababa, Ethiopia", "city": "Bahir Dar", "country": "Ethiopia"}
        self.assertEqual(self.pp._build_location_value(row), "Addis Ababa, Ethiopia")

    def test_fallback_to_city_country(self) -> None:
        row = {"city": "Addis Ababa", "country": "Ethiopia"}
        self.assertEqual(self.pp._build_location_value(row), "Addis Ababa, Ethiopia")

    def test_city_only(self) -> None:
        row = {"city": "Addis Ababa"}
        self.assertEqual(self.pp._build_location_value(row), "Addis Ababa")

    def test_no_location_info(self) -> None:
        row = {}
        self.assertEqual(self.pp._build_location_value(row), "")


if __name__ == "__main__":
    unittest.main()
