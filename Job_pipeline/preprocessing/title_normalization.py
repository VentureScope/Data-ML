"""Step 4: Title normalization module.

Purpose:
- Map noisy job titles to canonical roles from the roles taxonomy.

Pipeline behavior:
1. Load role taxonomy from `Job_pipeline/taxonomy/roles.json`
2. Build semantic embeddings using `sentence-transformers`
3. Score title+description against each role profile via cosine similarity
4. Apply title alignment boosts for better precision
5. If score < threshold (default 0.55), fallback to Gemini prompt extraction
6. Normalize fallback role text using alias + `rapidfuzz` matching
7. Return normalized_title + confidence_score + method_used

Usage:
- `TitleNormalizationModule().transform(record)` for pipeline usage.
- `normalize_title(title, description)` for one-off usage.

Notes:
- Primary method is semantic embedding similarity (`method_used=embedding`).
- If Gemini is unavailable and threshold is not met, returns
    `method_used=embedding_fallback_unavailable`.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from rapidfuzz import fuzz

from Job_pipeline.preprocessing.gemini_key_selector import select_random_gemini_api_key
from Job_pipeline.preprocessing.semantic_utils import SemanticEncoder


@dataclass
class TitleNormalizationConfig:
    """Configuration for title normalization."""

    roles_path: str = "Job_pipeline/taxonomy/roles.json"
    threshold: float = 0.55
    title_key: str = "clean_title"
    description_key: str = "clean_description"
    output_role_key: str = "normalized_title"
    output_confidence_key: str = "confidence_score"
    output_method_key: str = "method_used"
    default_method_name: str = "embedding"
    fallback_method_name: str = "gemini"
    include_similarity_debug: bool = False
    embedding_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    title_weight: int = 3
    title_exact_boost: float = 0.25
    title_partial_boost: float = 0.12


@dataclass
class RoleProfile:
    """In-memory role profile from taxonomy."""

    role_name: str
    description: str
    aliases: Sequence[str]


class TitleNormalizationModule:
    """Normalize job titles via semantic similarity with Gemini fallback."""

    def __init__(
        self,
        config: Optional[TitleNormalizationConfig] = None,
        gemini_callable: Optional[Callable[[str], str]] = None,
    ):
        self.config = config or TitleNormalizationConfig()
        self._roles = self._load_roles(self.config.roles_path)
        self._gemini_callable = gemini_callable
        self._encoder = SemanticEncoder(self.config.embedding_model_name)
        self._role_texts = [self._build_role_text(role) for role in self._roles]
        self._role_embeddings = self._encoder.encode(self._role_texts)

        if not 0.0 <= self.config.threshold <= 1.0:
            raise ValueError("threshold must be between 0.0 and 1.0")

    def _resolve_roles_path(self, roles_path: str) -> Path:
        direct = Path(roles_path)
        if direct.exists():
            return direct

        module_dir = Path(__file__).resolve().parent
        root_guess = module_dir.parent.parent
        candidate = root_guess / "taxonomy" / "roles.json"
        if candidate.exists():
            return candidate

        raise FileNotFoundError(f"Roles taxonomy not found: {roles_path}")

    def _load_roles(self, roles_path: str) -> List[RoleProfile]:
        path = self._resolve_roles_path(roles_path)
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)

        profiles: List[RoleProfile] = []
        for item in payload:
            profiles.append(
                RoleProfile(
                    role_name=str(item.get("role_name", "")).strip(),
                    description=str(item.get("description", "")).strip(),
                    aliases=[str(a).strip() for a in item.get("common_alternative_titles", []) if str(a).strip()],
                )
            )

        if not profiles:
            raise ValueError("roles taxonomy is empty")

        return profiles

    def _build_role_text(self, role: RoleProfile) -> str:
        alias_text = " ".join(role.aliases)
        return f"{role.role_name} {role.description} {alias_text}".strip()

    def _title_alignment_boost(self, title: str, role: RoleProfile) -> float:
        t = (title or "").strip().lower()
        if not t:
            return 0.0

        role_name = role.role_name.lower()
        aliases = [a.lower() for a in role.aliases]

        if t == role_name or t in aliases:
            return self.config.title_exact_boost

        if role_name in t or t in role_name:
            return self.config.title_partial_boost

        for alias in aliases:
            if alias in t or t in alias:
                return self.config.title_partial_boost

        return 0.0

    def _embedding_match(self, title: str, description: str) -> Tuple[str, float]:
        weighted_title = ((title or "") + " ") * max(self.config.title_weight, 1)
        query_text = f"{weighted_title}{description}".strip()
        query_vec = self._encoder.encode_one(query_text)
        sims = self._encoder.cosine_similarities(query_vec, self._role_embeddings)

        best_role = self._roles[0].role_name
        best_score = -1.0

        for idx, role in enumerate(self._roles):
            score = float(sims[idx])
            score += self._title_alignment_boost(title, role)
            score = min(score, 1.0)
            if score > best_score:
                best_score = score
                best_role = role.role_name

        return best_role, max(best_score, 0.0)

    def _build_gemini_prompt(self, title: str, description: str) -> str:
        roles = ", ".join(role.role_name for role in self._roles)
        return (
            "You are a job title normalization system.\n\n"
            "Map the following job posting to one of the standard roles:\n"
            f"[{roles}]\n\n"
            f"Job Title:\n{title}\n\n"
            f"Job Description:\n{description}\n\n"
            "Return only the normalized role name."
        )

    def _find_best_role_name(self, raw_role: str) -> Optional[str]:
        candidate = (raw_role or "").strip().lower()
        if not candidate:
            return None

        # Exact canonical match.
        for role in self._roles:
            if candidate == role.role_name.lower():
                return role.role_name

        # Alias match.
        for role in self._roles:
            if any(candidate == alias.lower() for alias in role.aliases):
                return role.role_name

        # Approximate fallback by fuzzy match against canonical names.
        best = None
        best_score = 0.0
        for role in self._roles:
            score = fuzz.ratio(candidate, role.role_name.lower()) / 100.0
            if score > best_score:
                best_score = score
                best = role.role_name

        return best if best_score >= 0.65 else None

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
            text = getattr(response, "text", None)
            if not text:
                return None
            return str(text).strip()
        except Exception:
            return None

    def normalize(self, title: Optional[str], description: Optional[str]) -> Dict[str, object]:
        """Normalize role using similarity first, then Gemini fallback if needed."""
        safe_title = (title or "").strip()
        safe_description = (description or "").strip()

        embedding_role, score = self._embedding_match(safe_title, safe_description)
        if score >= self.config.threshold:
            result: Dict[str, object] = {
                self.config.output_role_key: embedding_role,
                self.config.output_confidence_key: round(score, 4),
                self.config.output_method_key: self.config.default_method_name,
            }
            if self.config.include_similarity_debug:
                result["similarity_score"] = round(score, 4)
            return result

        prompt = self._build_gemini_prompt(safe_title, safe_description)
        gemini_raw = self._call_gemini(prompt)
        if gemini_raw:
            normalized = self._find_best_role_name(gemini_raw)
            if normalized:
                result = {
                    self.config.output_role_key: normalized,
                    self.config.output_confidence_key: "gemini",
                    self.config.output_method_key: self.config.fallback_method_name,
                }
                if self.config.include_similarity_debug:
                    result["similarity_score"] = round(score, 4)
                    result["gemini_raw"] = gemini_raw
                return result

        # If Gemini is unavailable/unusable, keep best embedding result.
        result = {
            self.config.output_role_key: embedding_role,
            self.config.output_confidence_key: round(score, 4),
            self.config.output_method_key: f"{self.config.default_method_name}_fallback_unavailable",
        }
        if self.config.include_similarity_debug:
            result["similarity_score"] = round(score, 4)
            result["gemini_raw"] = gemini_raw
        return result

    def transform(self, record: Dict[str, str]) -> Dict[str, object]:
        """Transform a record by attaching normalized title metadata."""
        output: Dict[str, object] = dict(record)
        output.update(
            self.normalize(
                title=record.get(self.config.title_key),
                description=record.get(self.config.description_key),
            )
        )
        return output


def normalize_title(
    title: Optional[str],
    description: Optional[str],
    config: Optional[TitleNormalizationConfig] = None,
    gemini_callable: Optional[Callable[[str], str]] = None,
) -> Dict[str, object]:
    """Convenience function for one-off title normalization."""
    module = TitleNormalizationModule(config=config, gemini_callable=gemini_callable)
    return module.normalize(title=title, description=description)


__all__ = [
    "TitleNormalizationConfig",
    "TitleNormalizationModule",
    "normalize_title",
]
