"""Step 6: Location extraction module.

Purpose:
- Extract structured geography fields from postings: city, region, country.

Pipeline behavior:
1. Rule/regex extraction from explicit location field (if available)
2. Rule/regex extraction from description text
3. NER extraction using spaCy (`en_core_web_sm`)
4. Gemini fallback when no confident location is found

Main outputs:
- city, region, country
- confidence_score
- method_used

Usage:
- `LocationExtractionModule().transform(record)` for pipeline usage.
- `extract_location(description, location_value=...)` for one-off extraction.

Notes:
- Returns `method_used` as `rule`, `ner`, or `gemini`.
- If Gemini is unavailable, the module returns the best rule/NER result.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import spacy
from Job_pipeline.preprocessing.gemini_key_selector import select_random_gemini_api_key


@dataclass
class LocationExtractionConfig:
    """Configuration for location extraction."""

    description_key: str = "clean_description"
    location_key: str = "location"
    output_city_key: str = "city"
    output_region_key: str = "region"
    output_country_key: str = "country"
    output_confidence_key: str = "confidence_score"
    output_method_key: str = "method_used"
    fallback_method_name: str = "gemini"
    default_method_name: str = "rule"
    ner_method_name: str = "ner"
    fallback_confidence: str = "gemini"


class LocationExtractionModule:
    """Extract location with rule/regex/NER and Gemini fallback."""

    _location_phrase_re = re.compile(
        r"\b(?:based in|located in|location|work location|in)\s+([A-Za-z\- ]+(?:,\s*[A-Za-z\- ]+)*)",
        flags=re.IGNORECASE,
    )

    _remote_re = re.compile(r"\b(remote|work from home|wfh|anywhere)\b", flags=re.IGNORECASE)

    _country_map = {
        "ethiopia": "Ethiopia",
        "kenya": "Kenya",
        "uganda": "Uganda",
        "rwanda": "Rwanda",
        "tanzania": "Tanzania",
        "nigeria": "Nigeria",
        "ghana": "Ghana",
        "south africa": "South Africa",
        "egypt": "Egypt",
    }

    _city_to_country = {
        "addis ababa": ("Addis Ababa", "Addis Ababa", "Ethiopia"),
        "adama": ("Adama", "Oromia", "Ethiopia"),
        "hawassa": ("Hawassa", "Sidama", "Ethiopia"),
        "bahir dar": ("Bahir Dar", "Amhara", "Ethiopia"),
        "mekelle": ("Mekelle", "Tigray", "Ethiopia"),
        "dire dawa": ("Dire Dawa", "Dire Dawa", "Ethiopia"),
        "nairobi": ("Nairobi", "Nairobi County", "Kenya"),
        "kampala": ("Kampala", "Kampala", "Uganda"),
        "kigali": ("Kigali", "Kigali", "Rwanda"),
        "lagos": ("Lagos", "Lagos", "Nigeria"),
        "accra": ("Accra", "Greater Accra", "Ghana"),
        "cape town": ("Cape Town", "Western Cape", "South Africa"),
        "johannesburg": ("Johannesburg", "Gauteng", "South Africa"),
        "cairo": ("Cairo", "Cairo", "Egypt"),
    }

    _nlp = None

    def __init__(
        self,
        config: Optional[LocationExtractionConfig] = None,
        gemini_callable: Optional[Callable[[str], str]] = None,
    ):
        self.config = config or LocationExtractionConfig()
        self._gemini_callable = gemini_callable

    def _blank_result(self) -> Dict[str, Optional[object]]:
        return {
            self.config.output_city_key: "",
            self.config.output_region_key: "",
            self.config.output_country_key: "",
            self.config.output_confidence_key: 0.0,
            self.config.output_method_key: self.config.default_method_name,
        }

    def _parse_location_string(self, raw: str) -> Optional[Tuple[str, str, str, float]]:
        text = (raw or "").strip()
        if not text:
            return None

        low = text.lower()
        if self._remote_re.search(low):
            return "", "", "", 0.0

        for city_key, triple in self._city_to_country.items():
            if city_key in low:
                city, region, country = triple
                return city, region, country, 0.95

        for country_key, country_name in self._country_map.items():
            if country_key in low:
                parts = [part.strip() for part in text.split(",") if part.strip()]
                city = parts[0] if len(parts) > 1 else ""
                region = parts[1] if len(parts) > 2 else ""
                return city, region, country_name, 0.75

        # Pattern-based extraction: "based in Addis Ababa, Ethiopia"
        match = self._location_phrase_re.search(text)
        if match:
            phrase = match.group(1).strip()
            parsed = [p.strip() for p in phrase.split(",") if p.strip()]
            if len(parsed) == 1:
                token_low = parsed[0].lower()
                if token_low in self._country_map:
                    return "", "", self._country_map[token_low], 0.65
                if token_low in self._city_to_country:
                    city, region, country = self._city_to_country[token_low]
                    return city, region, country, 0.8
                return parsed[0], "", "", 0.5

            if len(parsed) >= 2:
                city = parsed[0]
                country_token = parsed[-1].lower()
                country = self._country_map.get(country_token, parsed[-1])
                region = parsed[1] if len(parsed) > 2 else ""
                return city, region, country, 0.7

        return None

    def _extract_with_ner(self, text: str) -> Optional[Tuple[str, str, str, float]]:
        """Best-effort NER extraction using spaCy when available."""
        try:
            if self.__class__._nlp is None:
                self.__class__._nlp = spacy.load("en_core_web_sm")

            doc = self.__class__._nlp(text)
            gpes = [ent.text.strip() for ent in doc.ents if ent.label_ in {"GPE", "LOC"}]
            if not gpes:
                return None

            best = gpes[0]
            low = best.lower()
            if low in self._city_to_country:
                city, region, country = self._city_to_country[low]
                return city, region, country, 0.72
            if low in self._country_map:
                return "", "", self._country_map[low], 0.7

            return best, "", "", 0.6
        except Exception:
            return None

    def _build_gemini_prompt(self, text: str) -> str:
        return (
            "Extract the location from the job posting.\n\n"
            "Return JSON:\n"
            "{\n"
            '  "city": "",\n'
            '  "region": "",\n'
            '  "country": ""\n'
            "}\n\n"
            f"Job description:\n{text}"
        )

    def _call_gemini(self, prompt: str) -> Optional[Dict[str, str]]:
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

        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                return None
            return {
                "city": str(payload.get("city", "") or "").strip(),
                "region": str(payload.get("region", "") or "").strip(),
                "country": str(payload.get("country", "") or "").strip(),
            }
        except Exception:
            start = raw.find("{")
            end = raw.rfind("}")
            if start == -1 or end == -1 or end <= start:
                return None
            try:
                payload = json.loads(raw[start : end + 1])
                if not isinstance(payload, dict):
                    return None
                return {
                    "city": str(payload.get("city", "") or "").strip(),
                    "region": str(payload.get("region", "") or "").strip(),
                    "country": str(payload.get("country", "") or "").strip(),
                }
            except Exception:
                return None

    def extract(self, description: Optional[str], location_value: Optional[str] = None) -> Dict[str, object]:
        """Extract location fields from text with fallback behavior."""
        result = self._blank_result()

        # 1) Prefer explicit location field if available.
        explicit = self._parse_location_string(location_value or "")
        if explicit is not None:
            city, region, country, conf = explicit
            result.update(
                {
                    self.config.output_city_key: city,
                    self.config.output_region_key: region,
                    self.config.output_country_key: country,
                    self.config.output_confidence_key: conf,
                    self.config.output_method_key: self.config.default_method_name,
                }
            )
            if city or region or country:
                return result

        text = (description or "").strip()
        if not text:
            return result

        # 2) Rule/regex on description.
        rule_match = self._parse_location_string(text)
        if rule_match is not None:
            city, region, country, conf = rule_match
            result.update(
                {
                    self.config.output_city_key: city,
                    self.config.output_region_key: region,
                    self.config.output_country_key: country,
                    self.config.output_confidence_key: conf,
                    self.config.output_method_key: self.config.default_method_name,
                }
            )
            if city or region or country:
                return result

        # 3) Optional NER.
        ner_match = self._extract_with_ner(text)
        if ner_match is not None:
            city, region, country, conf = ner_match
            result.update(
                {
                    self.config.output_city_key: city,
                    self.config.output_region_key: region,
                    self.config.output_country_key: country,
                    self.config.output_confidence_key: conf,
                    self.config.output_method_key: self.config.ner_method_name,
                }
            )
            if city or region or country:
                return result

        # 4) Gemini fallback.
        prompt = self._build_gemini_prompt(text)
        gemini_payload = self._call_gemini(prompt)
        if gemini_payload:
            result.update(
                {
                    self.config.output_city_key: gemini_payload.get("city", ""),
                    self.config.output_region_key: gemini_payload.get("region", ""),
                    self.config.output_country_key: gemini_payload.get("country", ""),
                    self.config.output_confidence_key: self.config.fallback_confidence,
                    self.config.output_method_key: self.config.fallback_method_name,
                }
            )

        return result

    def transform(self, record: Dict[str, str]) -> Dict[str, object]:
        """Return record copy with extracted location fields attached."""
        output: Dict[str, object] = dict(record)
        output.update(
            self.extract(
                description=record.get(self.config.description_key),
                location_value=record.get(self.config.location_key),
            )
        )
        return output


def extract_location(
    description: Optional[str],
    location_value: Optional[str] = None,
    config: Optional[LocationExtractionConfig] = None,
    gemini_callable: Optional[Callable[[str], str]] = None,
) -> Dict[str, object]:
    """Convenience function for one-off location extraction."""
    module = LocationExtractionModule(config=config, gemini_callable=gemini_callable)
    return module.extract(description=description, location_value=location_value)


__all__ = [
    "LocationExtractionConfig",
    "LocationExtractionModule",
    "extract_location",
]
