"""Taxonomy-driven tech job validation.

This module determines whether a posting should be treated as a tech job
before running downstream preprocessing modules.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence


@dataclass
class TechJobValidationConfig:
    """Configuration for tech job validation."""

    roles_path: str = "Job_pipeline/taxonomy/roles.json"
    min_keyword_hits: int = 1
    title_boost_hits: int = 1


@dataclass
class _RoleProfile:
    role_name: str
    category: str
    aliases: Sequence[str]


class TechJobValidationModule:
    """Classify whether a posting is tech using roles taxonomy signals."""

    def __init__(self, config: TechJobValidationConfig | None = None):
        self.config = config or TechJobValidationConfig()
        self._roles = self._load_roles(self.config.roles_path)
        self._keywords = self._build_keywords(self._roles)

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

    def _load_roles(self, roles_path: str) -> List[_RoleProfile]:
        path = self._resolve_roles_path(roles_path)
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)

        roles: List[_RoleProfile] = []
        for item in payload:
            role_name = str(item.get("role_name", "")).strip()
            category = str(item.get("category", "")).strip()
            aliases = [
                str(alias).strip()
                for alias in item.get("common_alternative_titles", [])
                if str(alias).strip()
            ]
            if role_name:
                roles.append(_RoleProfile(role_name=role_name, category=category, aliases=aliases))

        if not roles:
            raise ValueError("roles taxonomy is empty")

        return roles

    def _normalize_text(self, text: str) -> str:
        text = re.sub(r'[\W_]+', ' ', (text or "").lower())
        text = text.replace("front end", "frontend")
        text = text.replace("back end", "backend")
        text = text.replace("full stack", "fullstack")
        text = text.replace("ui ux", "uiux")
        text = text.replace("ui and ux", "uiux")
        text = text.replace("dev ops", "devops")
        return f" {text.strip()} "

    def _build_keywords(self, roles: Sequence[_RoleProfile]) -> List[str]:
        terms: set[str] = set()
        for role in roles:
            terms.add(self._normalize_text(role.role_name))
            for alias in role.aliases:
                terms.add(self._normalize_text(alias))
        
        # Filter out overly generic words or empty strings
        ignore_list = {" other ", " "}
        terms = {t for t in terms if t not in ignore_list}
        
        return sorted(terms, key=len, reverse=True)

    def _count_matches(self, text: str) -> int:
        hay = self._normalize_text(text)
        if not hay.strip():
            return 0
        return sum(1 for keyword in self._keywords if keyword in hay)

    def _best_role(self, text: str) -> _RoleProfile:
        hay = self._normalize_text(text)

        best = self._roles[0]
        best_score = -1
        for role in self._roles:
            score = 0
            role_name = self._normalize_text(role.role_name)
            if role_name and role_name in hay:
                score += 2
            for alias in role.aliases:
                alias_norm = self._normalize_text(alias)
                if alias_norm and alias_norm in hay:
                    score += 1

            if score > best_score:
                best_score = score
                best = role

        return best

    def classify(self, title: str | None, description: str | None) -> Dict[str, object]:
        """Return taxonomy-based decision for whether row is a tech job."""
        safe_title = (title or "").strip()
        safe_description = (description or "").strip()
        joined = f"{safe_title} {safe_description}".strip()

        title_hits = self._count_matches(safe_title)
        all_hits = self._count_matches(joined)
        is_tech = all_hits >= self.config.min_keyword_hits and (
            title_hits >= self.config.title_boost_hits or all_hits >= self.config.min_keyword_hits + 1
        )

        best = self._best_role(joined)
        confidence = min(1.0, round((all_hits / 5.0) + (title_hits / 5.0), 4))
        method = "taxonomy_keyword_match"

        return {
            "is_tech": bool(is_tech),
            "matched_role": best.role_name,
            "matched_category": best.category,
            "confidence_score": confidence,
            "method_used": method,
            "keyword_hits": int(all_hits),
            "title_keyword_hits": int(title_hits),
        }


__all__ = ["TechJobValidationConfig", "TechJobValidationModule"]
