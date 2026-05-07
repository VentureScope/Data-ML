"""Unified preprocessing pipeline (Step 1 through Step 10).

This module orchestrates all implemented preprocessing steps in one place and
produces the target feature schema for a single job-post row.

Pipeline order:
1. Text cleaning
2. Job ID generation
3. Date features
4. Title normalization
5. Description embedding
6. Location extraction
7. Remote detection
8. Job type extraction
9. Education extraction
10. Skills extraction
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Dict, List, Optional

from Job_pipeline.preprocessing.clean_text import CleanTextModule
from Job_pipeline.preprocessing.date_features import DateFeaturesConfig, DateFeaturesModule
from Job_pipeline.preprocessing.description_embedding import DescriptionEmbeddingModule
from Job_pipeline.preprocessing.education_extraction import EducationExtractionModule
from Job_pipeline.preprocessing.job_id import JobIdConfig, JobIdModule
from Job_pipeline.preprocessing.job_type_extraction import JobTypeExtractionModule
from Job_pipeline.preprocessing.location_extraction import LocationExtractionModule
from Job_pipeline.preprocessing.remote_detection import RemoteDetectionModule
from Job_pipeline.preprocessing.groq_client import RobustGroqClient
from Job_pipeline.preprocessing.skills_extraction import SkillsExtractionModule
from Job_pipeline.preprocessing.tech_job_validation import TechJobValidationModule
from Job_pipeline.preprocessing.title_normalization import TitleNormalizationModule


logger = logging.getLogger(__name__)


TARGET_FEATURES = [
    "year_month",
    "timestamp",
    "month",
    "holiday_flag",
    "job_id",
    "company_name",
    "job_title",
    "normalized_title",
    "DescriptionVec",
    "city",
    "region",
    "country",
    "is_remote",
    "job_type",
    "education_level",
    "skills",
]


# Mapping of canonical keys to possible alternate keys found in different sources.
_FIELD_ALIASES: Dict[str, list[str]] = {
    "created_at": ["posted_date", "date", "publish_date", "published_date", "scraped_at"],
    "title": ["job_title", "position", "role"],
    "description": ["job_description", "desc", "body"],
    "entity_name": ["company_name", "company", "employer", "organization"],
    # Structured location fields from JSON sources.
    "location": ["work_location"],
    # Structured hint fields — pre-parsed values from JSON data sources.
    "job_type_hint": ["job_type"],
    "job_site_hint": ["job_site"],
    "skills_hint": ["skills"],
    "education_hint": ["education_qualification"],
}


def _resolve_key(row: Dict[str, object], canonical: str) -> object | None:
    """Return the value from *row* using the canonical key or any known alias."""
    val = row.get(canonical)
    if val is not None:
        return val
    for alias in _FIELD_ALIASES.get(canonical, []):
        val = row.get(alias)
        if val is not None:
            return val
    return None


@dataclass
class UnifiedPreprocessorConfig:
    """Configuration for unified preprocessing."""

    date_key: str = "created_at"
    title_key: str = "title"
    description_key: str = "description"
    company_key: str = "entity_name"
    source_key: str = "source"
    enable_llm_fallback: bool = False


class UnifiedPreprocessor:
    """Run all preprocessing modules and emit the final feature contract."""

    def __init__(self, config: Optional[UnifiedPreprocessorConfig] = None):
        self.config = config or UnifiedPreprocessorConfig()

        gemini_callable = None
        if self.config.enable_llm_fallback:
            client = RobustGroqClient()
            gemini_callable = client
        else:
            gemini_callable = lambda _prompt: None

        self.clean_text = CleanTextModule()
        self.tech_validator = TechJobValidationModule()
        self.job_id = JobIdModule(
            JobIdConfig(
                title_key=self.config.title_key,
                company_key=self.config.company_key,
                date_key=self.config.date_key,
                source_key=self.config.source_key,
            )
        )
        self.date_features = DateFeaturesModule(DateFeaturesConfig(date_key=self.config.date_key))
        self.title_normalizer = TitleNormalizationModule(gemini_callable=gemini_callable)
        self.description_embedding = DescriptionEmbeddingModule()
        self.location_extractor = LocationExtractionModule(gemini_callable=gemini_callable)
        self.remote_detector = RemoteDetectionModule(gemini_callable=gemini_callable)
        self.job_type_extractor = JobTypeExtractionModule(gemini_callable=gemini_callable)
        self.education_extractor = EducationExtractionModule(gemini_callable=gemini_callable)
        self.skills_extractor = SkillsExtractionModule(gemini_callable=gemini_callable)
        logger.info(
            "UnifiedPreprocessor initialized; llm_fallback_enabled=%s",
            self.config.enable_llm_fallback,
        )

    def _build_location_value(self, row: Dict[str, object]) -> str:
        """Build a location string for the location extractor.

        Priority: explicit ``location``/``work_location`` field from the JSON
        source → legacy ``city``+``country`` keys from CSV sources.
        """
        # Prefer the resolved location field (from JSON sources).
        explicit = str(row.get("location") or "").strip()
        if explicit:
            return explicit

        city = str(row.get("city") or "").strip()
        country = str(row.get("country") or "").strip()
        if city and country:
            return f"{city}, {country}"
        return city or country

    @staticmethod
    def _best_description(working: Dict[str, object], desc_key: str) -> None:
        """Prefer the longer of description vs raw_text to avoid truncation.

        Many Telegram-scraped JSON rows have a truncated ``description`` while
        ``raw_text`` contains the full posting.  If ``raw_text`` exists and is
        longer, replace the description with it.
        """
        raw_text = str(working.get("raw_text") or "").strip()
        current = str(working.get(desc_key) or "").strip()
        if raw_text and len(raw_text) > len(current):
            working[desc_key] = raw_text

    def preprocess_row(
        self, row: Dict[str, object], source_name: Optional[str] = None
    ) -> Optional[Dict[str, object]]:
        """Preprocess one raw row and return target features only."""
        working: Dict[str, object] = dict(row)
        logger.debug("preprocess_row start source_name=%s row_keys=%d", source_name, len(working))

        # --- Resolve alternate field names from different sources ---
        for canonical in (self.config.date_key, self.config.title_key,
                          self.config.description_key, self.config.company_key,
                          "location", "job_type_hint", "job_site_hint",
                          "skills_hint", "education_hint"):
            if working.get(canonical) is None:
                resolved = _resolve_key(working, canonical)
                if resolved is not None:
                    working[canonical] = resolved

        # --- Early exit: non-job rows (polls, announcements) ---
        title_val = working.get(self.config.title_key)
        desc_val = working.get(self.config.description_key)
        raw_text_val = working.get("raw_text")
        if not title_val and not desc_val and not raw_text_val:
            logger.info(
                "preprocess_row skipped_empty_row source_name=%s",
                source_name,
            )
            return None

        # --- Prefer the longer of description vs raw_text ---
        self._best_description(working, self.config.description_key)

        if source_name and not working.get(self.config.source_key):
            working[self.config.source_key] = source_name
        if not working.get(self.config.source_key):
            working[self.config.source_key] = "unknown"

        cleaned = self.clean_text.transform(
            title=str(working.get(self.config.title_key) or ""),
            description=str(working.get(self.config.description_key) or ""),
        )
        working.update(cleaned)

        tech_decision = self.tech_validator.classify(
            title=working.get("clean_title"),
            description=working.get("clean_description"),
        )
        if not tech_decision.get("is_tech", False):
            logger.info(
                "preprocess_row skipped_non_tech source=%s title=%s category=%s score=%s",
                working.get(self.config.source_key),
                working.get(self.config.title_key),
                tech_decision.get("matched_category"),
                tech_decision.get("confidence_score"),
            )
            return None

        # --- Resolve structured hints for downstream modules ---
        job_type_hint: Optional[str] = None
        jth = working.get("job_type_hint")
        if isinstance(jth, str) and jth.strip():
            job_type_hint = jth.strip()

        job_site_hint: Optional[str] = None
        jsh = working.get("job_site_hint")
        if isinstance(jsh, str) and jsh.strip():
            job_site_hint = jsh.strip()
        # no_link_jobs.json embeds work mode in the job_type field itself,
        # e.g. "On-site - Permanent (Full-time)" or "Remote - Contractual".
        # If no explicit job_site_hint, try the job_type_hint as remote hint.
        if not job_site_hint and job_type_hint:
            job_site_hint = job_type_hint

        skills_hint: Optional[List[object]] = None
        sh = working.get("skills_hint")
        if isinstance(sh, list) and sh:
            skills_hint = sh

        education_hint: Optional[str] = None
        eh = working.get("education_hint")
        if isinstance(eh, str) and eh.strip():
            education_hint = eh.strip()

        # --- Run pipeline steps ---
        date_out = self.date_features.transform(working)
        id_out = self.job_id.transform(working)
        title_out = self.title_normalizer.normalize(
            title=working.get("clean_title"),
            description=working.get("clean_description"),
        )
        desc_vec_out = self.description_embedding.transform(
            {"clean_description": working.get("clean_description", "")}
        )
        location_out = self.location_extractor.extract(
            description=working.get("clean_description"),
            location_value=self._build_location_value(working),
        )
        remote_out = self.remote_detector.detect(
            title=working.get("clean_title"),
            description=working.get("clean_description"),
            hint=job_site_hint,
        )
        job_type_out = self.job_type_extractor.extract(
            title=working.get("clean_title"),
            description=working.get("clean_description"),
            hint=job_type_hint,
        )
        education_out = self.education_extractor.extract(
            title=working.get("clean_title"),
            description=working.get("clean_description"),
            hint=education_hint,
        )
        skills_out = self.skills_extractor.extract(
            working.get("clean_description"),
            hint_skills=skills_hint,
        )

        output = {
            "year_month": date_out.get("year_month"),
            "timestamp": date_out.get("timestamp"),
            "month": date_out.get("month"),
            "holiday_flag": date_out.get("holiday_flag"),
            "job_id": id_out.get("job_id"),
            "company_name": working.get(self.config.company_key, ""),
            "job_title": working.get(self.config.title_key, ""),
            "normalized_title": title_out.get("normalized_title"),
            "DescriptionVec": desc_vec_out.get("DescriptionVec"),
            "city": location_out.get("city", ""),
            "region": location_out.get("region", ""),
            "country": location_out.get("country", ""),
            "is_remote": remote_out.get("is_remote", False),
            "job_type": job_type_out.get("job_type", "not_specified"),
            "education_level": education_out.get("education_level", "Not specified"),
            "skills": skills_out.get("skills", []),
        }
        logger.info(
            "preprocess_row complete source=%s job_id=%s title=%s skills_count=%d",
            working.get(self.config.source_key),
            output.get("job_id"),
            output.get("normalized_title"),
            len(output.get("skills") or []),
        )
        return output


__all__ = ["UnifiedPreprocessorConfig", "UnifiedPreprocessor", "TARGET_FEATURES"]
