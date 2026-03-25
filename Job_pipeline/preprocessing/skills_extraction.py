"""Step 10: Skills extraction module.

Purpose:
- Extract a high-signal list of skills from job descriptions.

Pipeline behavior:
1. Load skills taxonomy from `Job_pipeline/taxonomy/skills.json`
2. Build semantic embeddings using `sentence-transformers`
3. Score taxonomy skills via cosine similarity + mention-aware boosting
4. Keep candidates above threshold with mention-aware precision gating
5. If detected skills < minimum threshold (default 2), fallback to Gemini
6. Normalize fallback skills with canonical + `rapidfuzz` matching
7. Return normalized skills list with method and confidence metadata

Main outputs:
- skills (list)
- skills_count
- confidence_score
- method_used

Usage:
- `SkillsExtractionModule().transform(record)` for pipeline usage.
- `extract_skills(description)` for one-off usage.

Notes:
- Primary method is semantic embedding extraction (`method_used=embedding`).
- If Gemini is unavailable, returns best local extraction with
    `method_used=embedding_fallback_unavailable`.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from rapidfuzz import fuzz

from Job_pipeline.preprocessing.gemini_key_selector import select_random_gemini_api_key
from Job_pipeline.preprocessing.semantic_utils import SemanticEncoder


@dataclass
class SkillsExtractionConfig:
    """Configuration for skills extraction."""

    skills_path: str = "Job_pipeline/taxonomy/skills.json"
    description_key: str = "clean_description"
    output_skills_key: str = "skills"
    output_count_key: str = "skills_count"
    output_confidence_key: str = "confidence_score"
    output_method_key: str = "method_used"
    default_method_name: str = "embedding"
    fallback_method_name: str = "gemini"
    embedding_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    similarity_threshold: float = 0.45
    min_detected_skills: int = 2
    max_skills: int = 12


@dataclass
class SkillProfile:
    """In-memory skill profile from taxonomy."""

    skill_name: str
    description: str
    related_skills: Sequence[str]


class SkillsExtractionModule:
    """Extract skills using semantic embeddings with Gemini fallback."""

    _token_re = re.compile(r"[a-zA-Z0-9+#.]+")
    _skill_embedding_cache: Dict[Tuple[str, str], Tuple[List[str], object]] = {}

    def __init__(
        self,
        config: Optional[SkillsExtractionConfig] = None,
        gemini_callable: Optional[Callable[[str], str]] = None,
    ):
        self.config = config or SkillsExtractionConfig()
        self._skills = self._load_skills(self.config.skills_path)
        self._gemini_callable = gemini_callable
        self._encoder = SemanticEncoder(self.config.embedding_model_name)

        cache_key = (self.config.skills_path, self.config.embedding_model_name)
        cached = self.__class__._skill_embedding_cache.get(cache_key)
        if cached is not None:
            self._skill_texts, self._skill_embeddings = cached
        else:
            self._skill_texts = [self._skill_text(p) for p in self._skills]
            self._skill_embeddings = self._encoder.encode(self._skill_texts)
            self.__class__._skill_embedding_cache[cache_key] = (self._skill_texts, self._skill_embeddings)

        if not 0.0 <= self.config.similarity_threshold <= 1.0:
            raise ValueError("similarity_threshold must be between 0.0 and 1.0")
        if self.config.min_detected_skills < 1:
            raise ValueError("min_detected_skills must be >= 1")
        if self.config.max_skills < 1:
            raise ValueError("max_skills must be >= 1")

    def _resolve_skills_path(self, skills_path: str) -> Path:
        direct = Path(skills_path)
        if direct.exists():
            return direct

        module_dir = Path(__file__).resolve().parent
        fallback = module_dir.parent / "taxonomy" / "skills.json"
        if fallback.exists():
            return fallback

        raise FileNotFoundError(f"Skills taxonomy not found: {skills_path}")

    def _load_skills(self, skills_path: str) -> List[SkillProfile]:
        path = self._resolve_skills_path(skills_path)
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)

        profiles: List[SkillProfile] = []
        for item in payload:
            skill_name = str(item.get("skill_name", "")).strip()
            if not skill_name:
                continue
            profiles.append(
                SkillProfile(
                    skill_name=skill_name,
                    description=str(item.get("description", "") or "").strip(),
                    related_skills=[str(s).strip() for s in item.get("related_skills", []) if str(s).strip()],
                )
            )

        if not profiles:
            raise ValueError("skills taxonomy is empty")

        return profiles

    def _skill_text(self, profile: SkillProfile) -> str:
        return f"{profile.skill_name} {profile.description} {' '.join(profile.related_skills)}".strip()

    def _contains_phrase(self, text: str, phrase: str) -> bool:
        p = (phrase or "").strip().lower()
        if not p:
            return False
        if " " in p or "/" in p or "+" in p or "-" in p:
            return p in text.lower()
        return re.search(rf"\b{re.escape(p)}\b", text, flags=re.IGNORECASE) is not None

    def _mention_boost(self, description: str, profile: SkillProfile) -> float:
        if self._contains_phrase(description, profile.skill_name):
            return 0.45
        return 0.0

    def _embedding_extract(self, description: str) -> List[Tuple[str, float]]:
        qvec = self._encoder.encode_one(description)
        sem_scores = self._encoder.cosine_similarities(qvec, self._skill_embeddings)
        results: List[Tuple[str, float]] = []

        for idx, profile in enumerate(self._skills):
            score = float(sem_scores[idx])
            mention_boost = self._mention_boost(description, profile)
            score += mention_boost
            score = min(score, 1.0)
            # Require explicit mention or very strong semantic similarity.
            is_mentioned = mention_boost > 0.0
            if (is_mentioned and score >= 0.30) or score >= 0.62:
                results.append((profile.skill_name, round(score, 4)))

        results.sort(key=lambda x: x[1], reverse=True)
        deduped: List[Tuple[str, float]] = []
        seen = set()
        for name, score in results:
            if name in seen:
                continue
            seen.add(name)
            deduped.append((name, score))
            if len(deduped) >= self.config.max_skills:
                break
        return deduped

    def _build_gemini_prompt(self, text: str) -> str:
        return (
            "Extract the key skills from this job posting.\n\n"
            "Return JSON list:\n"
            '["Python", "SQL", "Machine Learning", ...]\n\n'
            f"Job description:\n{text}"
        )

    def _normalize_skill_name(self, raw: str) -> Optional[str]:
        clean = (raw or "").strip()
        candidate = clean.lower()
        if not candidate:
            return None

        for profile in self._skills:
            if candidate == profile.skill_name.lower():
                return profile.skill_name

        # approximate fallback via fuzzy canonical matching
        best_name = None
        best_score = 0.0
        for profile in self._skills:
            score = fuzz.ratio(candidate, profile.skill_name.lower()) / 100.0
            if score > best_score:
                best_score = score
                best_name = profile.skill_name

        if best_score >= 0.85:
            return best_name
        # Keep the model-provided skill when canonical mapping is uncertain.
        return clean

    def _call_gemini(self, prompt: str) -> Optional[List[str]]:
        raw: Optional[str] = None

        if self._gemini_callable is not None:
            try:
                raw = self._gemini_callable(prompt)
            except Exception:
                raw = None
        else:
            api_key = select_random_gemini_api_key()
            if api_key:
                try:
                    from google import genai  # type: ignore

                    model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
                    client = genai.Client(api_key=api_key)
                    response = client.models.generate_content(model=model, contents=prompt)
                    raw = str(getattr(response, "text", "") or "").strip()
                except Exception:
                    raw = None

        if not raw:
            return None

        def parse_list(candidate: str) -> Optional[List[str]]:
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, list):
                    return [str(x).strip() for x in parsed if str(x).strip()]
                return None
            except Exception:
                return None

        direct = parse_list(raw)
        if direct is not None:
            return direct

        start = raw.find("[")
        end = raw.rfind("]")
        if start == -1 or end == -1 or end <= start:
            return None
        return parse_list(raw[start : end + 1])

    def extract(self, description: Optional[str]) -> Dict[str, object]:
        """Extract skills with embedding first, then Gemini fallback."""
        text = (description or "").strip()
        if not text:
            return {
                self.config.output_skills_key: [],
                self.config.output_count_key: 0,
                self.config.output_confidence_key: 0.0,
                self.config.output_method_key: f"{self.config.default_method_name}_fallback_unavailable",
            }

        scored = self._embedding_extract(text)
        skill_names = [name for name, _ in scored]

        if len(skill_names) >= self.config.min_detected_skills:
            avg_conf = sum(score for _, score in scored) / max(len(scored), 1)
            return {
                self.config.output_skills_key: skill_names,
                self.config.output_count_key: len(skill_names),
                self.config.output_confidence_key: round(avg_conf, 4),
                self.config.output_method_key: self.config.default_method_name,
            }

        prompt = self._build_gemini_prompt(text)
        raw_skills = self._call_gemini(prompt)
        if raw_skills:
            normalized: List[str] = []
            seen = set()
            for item in raw_skills:
                canonical = self._normalize_skill_name(item)
                if canonical and canonical not in seen:
                    seen.add(canonical)
                    normalized.append(canonical)
                if len(normalized) >= self.config.max_skills:
                    break

            if normalized:
                return {
                    self.config.output_skills_key: normalized,
                    self.config.output_count_key: len(normalized),
                    self.config.output_confidence_key: "gemini",
                    self.config.output_method_key: self.config.fallback_method_name,
                }

        return {
            self.config.output_skills_key: skill_names,
            self.config.output_count_key: len(skill_names),
            self.config.output_confidence_key: round(sum(s for _, s in scored) / max(len(scored), 1), 4)
            if scored
            else 0.0,
            self.config.output_method_key: f"{self.config.default_method_name}_fallback_unavailable",
        }

    def transform(self, record: Dict[str, str]) -> Dict[str, object]:
        """Return a copy of record with extracted skill fields attached."""
        output: Dict[str, object] = dict(record)
        output.update(self.extract(record.get(self.config.description_key)))
        return output


def extract_skills(
    description: Optional[str],
    config: Optional[SkillsExtractionConfig] = None,
    gemini_callable: Optional[Callable[[str], str]] = None,
) -> Dict[str, object]:
    """Convenience function for one-off skill extraction."""
    module = SkillsExtractionModule(config=config, gemini_callable=gemini_callable)
    return module.extract(description)


__all__ = [
    "SkillsExtractionConfig",
    "SkillsExtractionModule",
    "extract_skills",
]
