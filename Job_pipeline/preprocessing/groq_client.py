"""Groq LLM client with multi-model cascade, key rotation, and proactive rate-limit throttling.

Model priority (by RPD):
  1. llama-3.1-8b-instant     — 14,400 RPD, 6K TPM  (workhorse)
  2. qwen/qwen3-32b           — 1,000 RPD, 60 RPM, 6K TPM (higher RPM, smarter)
  3. meta-llama/llama-4-scout-17b-16e-instruct — 1,000 RPD, 30K TPM (large context fallback)

Throttling strategy:
  - Read x-ratelimit-remaining-requests and x-ratelimit-reset-requests headers
    on every response to proactively slow down before hitting 429s.
  - On 429, mark that model exhausted and cascade to the next.
  - Exponential backoff per model slot.
"""
from __future__ import annotations

import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from groq import Groq, RateLimitError, APIStatusError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model cascade definition
# ---------------------------------------------------------------------------
MODEL_CASCADE: List[Tuple[str, int]] = [
    ("llama-3.1-8b-instant",                        500),   # primary  — 14,400 RPD
    ("qwen/qwen3-32b",                               500),   # secondary — 60 RPM
    ("meta-llama/llama-4-scout-17b-16e-instruct",   800),   # tertiary  — 30K TPM
]

# Warn when remaining requests drop below this fraction of the limit
_PROACTIVE_THRESHOLD = 0.10   # 10% remaining → start sleeping
_PROACTIVE_SLEEP = 2.0        # seconds to sleep proactively


# ---------------------------------------------------------------------------
# .env key loader
# ---------------------------------------------------------------------------

def _parse_key_list(raw: str) -> List[str]:
    text = (raw or "").strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    items: List[str] = []
    for piece in text.replace("\n", ",").split(","):
        token = piece.strip().strip("'\"")
        if token:
            items.append(token)
    return items


def _read_dotenv(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        values[k.strip()] = v.strip().strip("'\"")
    return values


def _load_dotenv() -> Dict[str, str]:
    merged: Dict[str, str] = {}
    module_dir = Path(__file__).resolve().parent
    for p in [
        Path.cwd() / ".env",
        module_dir / ".env",
        module_dir.parent / ".env",
        module_dir.parent.parent / ".env",
    ]:
        merged.update(_read_dotenv(p))
    return merged


def get_all_groq_api_keys() -> List[str]:
    """Collect Groq keys from environment and .env file."""
    keys: List[str] = []
    dotenv = _load_dotenv()
    for src in (os.environ, dotenv):  # type: ignore[list-item]
        raw = src.get("GROQ_API_KEYS")
        if raw:
            keys.extend(_parse_key_list(raw))
        for i in range(1, 51):
            k = src.get(f"GROQ_API_KEY_{i}")
            if k:
                keys.append(k.strip())
        single = src.get("GROQ_API_KEY")
        if single:
            keys.extend(_parse_key_list(single))

    deduped: List[str] = []
    seen: set = set()
    for k in keys:
        if k and k not in seen:
            seen.add(k)
            deduped.append(k)
    logger.debug("Discovered %d unique Groq API keys", len(deduped))
    return deduped


# ---------------------------------------------------------------------------
# Rate-limit header parsing
# ---------------------------------------------------------------------------

def _parse_reset_seconds(reset_str: Optional[str]) -> float:
    """Parse values like '2s', '1m30s', '500ms' into seconds."""
    if not reset_str:
        return 0.0
    total = 0.0
    for value, unit in re.findall(r"([\d.]+)(ms|m|s)", reset_str):
        v = float(value)
        if unit == "ms":
            total += v / 1000
        elif unit == "s":
            total += v
        elif unit == "m":
            total += v * 60
    return total


def _check_headers_proactive(response_headers: dict, model: str) -> None:
    """Sleep proactively if remaining requests are near the limit."""
    remaining_str = response_headers.get("x-ratelimit-remaining-requests")
    limit_str = response_headers.get("x-ratelimit-limit-requests")
    reset_str = response_headers.get("x-ratelimit-reset-requests")

    if remaining_str is None or limit_str is None:
        return
    try:
        remaining = int(remaining_str)
        limit = int(limit_str)
    except ValueError:
        return

    if limit == 0:
        return

    fraction_left = remaining / limit
    if fraction_left < _PROACTIVE_THRESHOLD:
        reset_secs = _parse_reset_seconds(reset_str)
        sleep_time = max(_PROACTIVE_SLEEP, min(reset_secs, 30.0))
        logger.info(
            "Groq proactive throttle model=%s remaining=%d/%d (%.0f%%) — sleeping %.1fs",
            model, remaining, limit, fraction_left * 100, sleep_time,
        )
        time.sleep(sleep_time)


# ---------------------------------------------------------------------------
# Main client
# ---------------------------------------------------------------------------

class RobustGroqClient:
    """Multi-model cascade Groq client with proactive rate-limit throttling.

    Implements the same ``__call__(prompt) -> Optional[str]`` interface as
    ``RobustGeminiClient`` so it drops in as the ``gemini_callable`` argument
    throughout the pipeline.
    """

    def __init__(self, cascade: Optional[List[Tuple[str, int]]] = None):
        self.cascade = cascade or MODEL_CASCADE
        self.keys = get_all_groq_api_keys()
        if not self.keys:
            logger.warning("No Groq API keys found. LLM fallback will fail immediately.")

        # Per-(key, model) backoff: value is the epoch time when it becomes available
        self.backoffs: Dict[Tuple[str, str], float] = {}
        self.failures: Dict[Tuple[str, str], int] = {}

    # ------------------------------------------------------------------
    def _backoff_key(self, key: str, model: str) -> Tuple[str, str]:
        return (key, model)

    def _is_available(self, key: str, model: str) -> bool:
        return self.backoffs.get((key, model), 0.0) <= time.time()

    def _mark_failed(self, key: str, model: str, status_code: Optional[int] = None) -> None:
        slot = (key, model)
        self.failures[slot] = self.failures.get(slot, 0) + 1
        failures = self.failures[slot]
        base = 60 if status_code == 429 else 10
        import random
        jitter = random.uniform(0.8, 1.2)
        delay = min(base * (2 ** min(failures - 1, 5)) * jitter, 600)
        self.backoffs[slot] = time.time() + delay
        logger.warning(
            "Groq key ...%s / model=%s failed (status=%s). Backoff %.1fs (failures=%d)",
            key[-4:], model, status_code, delay, failures,
        )

    def _mark_success(self, key: str, model: str) -> None:
        slot = (key, model)
        if slot in self.failures and self.failures[slot] > 0:
            self.failures[slot] -= 1

    # ------------------------------------------------------------------
    def __call__(self, prompt: str) -> Optional[str]:
        if not self.keys:
            return None

        # Try each model in cascade order
        for model, max_tokens in self.cascade:
            result = self._try_model(prompt, model, max_tokens)
            if result is not None:
                return result
            # model exhausted / all keys backed off — try next

        logger.error("Groq: all models in cascade exhausted for this prompt.")
        return None

    def _try_model(self, prompt: str, model: str, max_tokens: int) -> Optional[str]:
        """Attempt to get a response using a specific model, rotating keys."""
        import random

        max_attempts = 4
        for attempt in range(max_attempts):
            # Pick an available key for this model
            available = [k for k in self.keys if self._is_available(k, model)]
            if not available:
                # All keys for this model are backed off — skip to next model
                logger.info("Groq model=%s: all keys backed off, cascading.", model)
                return None

            key = random.choice(available)
            try:
                client = Groq(api_key=key)
                response = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                    max_tokens=max_tokens,
                )
                self._mark_success(key, model)

                # Proactive throttle check from response headers
                headers = getattr(getattr(response, "_raw_response", None), "headers", {})
                if not headers:
                    # Try via the httpx response attribute name used by groq SDK
                    headers = getattr(response, "headers", {})
                if headers:
                    _check_headers_proactive(dict(headers), model)

                return (response.choices[0].message.content or "").strip()

            except RateLimitError as e:
                logger.info("Groq 429 on key ...%s model=%s", key[-4:], model)
                # Try to extract reset time from exception headers
                headers = getattr(getattr(e, "response", None), "headers", {})
                reset_str = dict(headers).get("x-ratelimit-reset-requests") if headers else None
                reset_secs = _parse_reset_seconds(reset_str) if reset_str else None
                self._mark_failed(key, model, status_code=429)
                if reset_secs and reset_secs < 10:
                    # Short wait hinted by server — honour it before cascading
                    logger.info("Server hints reset in %.1fs — waiting.", reset_secs)
                    time.sleep(reset_secs + 0.5)
                # cascade to next model immediately
                return None

            except APIStatusError as e:
                code = e.status_code
                if code in (500, 502, 503, 504):
                    logger.warning("Groq server error %d key ...%s model=%s", code, key[-4:], model)
                    self._mark_failed(key, model, status_code=code)
                    time.sleep(2)
                    continue  # retry same model
                else:
                    logger.error("Groq unrecoverable error (status=%d): %s", code, e)
                    return None

            except Exception as e:
                err = str(e).lower()
                if "429" in err or "rate" in err:
                    self._mark_failed(key, model, status_code=429)
                    return None
                logger.exception("Unexpected Groq error key ...%s model=%s: %s", key[-4:], model, e)
                self._mark_failed(key, model)

        return None


# ---------------------------------------------------------------------------
# Batched callable wrapper
# ---------------------------------------------------------------------------

_BATCH_SYSTEM_PROMPT = """You are a data extraction assistant for job postings.
Extract the requested fields and return ONLY valid JSON. No explanation, no markdown."""

_BATCH_USER_TEMPLATE = """From this job posting extract all fields below and return as a single JSON object:

{{
  "city": "city name or empty string",
  "region": "region/state/province or empty string",
  "country": "country name or empty string",
  "is_remote": "remote | hybrid | onsite",
  "job_type": "full_time | part_time | internship | contractual | temporary | freelance",
  "education": "PhD | Masters | Bachelors | Diploma | Not specified",
  "skills": ["Skill1", "Skill2"]
}}

Job posting:
{text}"""

# Keywords used to detect which field a module prompt is asking about
_FIELD_SIGNALS: List[Tuple[str, str]] = [
    ("city",       "location"),
    ("region",     "location"),
    ("country",    "location"),
    ("remote",     "is_remote"),
    ("hybrid",     "is_remote"),
    ("onsite",     "is_remote"),
    ("job type",   "job_type"),
    ("full_time",  "job_type"),
    ("full-time",  "job_type"),
    ("internship", "job_type"),
    ("education",  "education"),
    ("bachelors",  "education"),
    ("masters",    "education"),
    ("skills",     "skills"),
    ("key skills", "skills"),
]


def _detect_field(prompt: str) -> Optional[str]:
    """Guess which field a module prompt is targeting."""
    lower = prompt.lower()
    for signal, field in _FIELD_SIGNALS:
        if signal in lower:
            return field
    return None


def _extract_job_text(prompt: str) -> str:
    """Pull the job description text out of a module prompt."""
    # All module prompts end with 'Job description:\n<text>' or 'Job posting:\n<text>'
    for marker in ("job description:\n", "job posting:\n"):
        idx = prompt.lower().find(marker)
        if idx != -1:
            return prompt[idx + len(marker):].strip()
    # Fallback: last 2/3 of the prompt is likely the text
    return prompt[len(prompt) // 3:].strip()


class BatchedGroqCallable:
    """Wraps RobustGroqClient to consolidate up to 5 per-module LLM calls into one.

    Usage
    -----
    Replace the per-row ``gemini_callable`` with a fresh ``BatchedGroqCallable``
    instance before processing each row::

        batcher = BatchedGroqCallable(groq_client)
        # pass batcher as gemini_callable to all modules
        result = preprocessor.preprocess_row(row, llm_callable=batcher)

    Or, when modules share the same callable reference (current impl), just
    create one batcher per row and pass it in. The batcher fires a single
    consolidated request on the FIRST call and serves the remaining calls from
    cache.
    """

    def __init__(self, client: RobustGroqClient):
        self._client = client
        self._cache: Optional[Dict[str, object]] = None  # populated on first call
        self._raw_text: Optional[str] = None            # job text extracted from first prompt
        self._exhausted: bool = False                   # True if consolidated call failed

    # ------------------------------------------------------------------
    def _fire_consolidated(self, job_text: str) -> Optional[Dict[str, object]]:
        prompt = _BATCH_USER_TEMPLATE.format(text=job_text)
        # Prepend system prompt as a user-turn prefix (some models ignore system role)
        full_prompt = f"{_BATCH_SYSTEM_PROMPT}\n\n{prompt}"
        raw = self._client(full_prompt)
        if not raw:
            return None
        # Strip markdown fences if any
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            lines = cleaned.splitlines()
            cleaned = "\n".join(
                l for l in lines if not l.strip().startswith("```")
            ).strip()
        try:
            import json as _json
            return _json.loads(cleaned)
        except Exception:
            logger.warning("BatchedGroqCallable: failed to parse consolidated JSON: %r", cleaned[:200])
            return None

    # ------------------------------------------------------------------
    def __call__(self, prompt: str) -> Optional[str]:
        field = _detect_field(prompt)

        # On first call, extract job text and fire the consolidated request
        if self._cache is None and not self._exhausted:
            job_text = _extract_job_text(prompt)
            self._raw_text = job_text
            result = self._fire_consolidated(job_text)
            if result is not None:
                self._cache = result
            else:
                self._exhausted = True

        # If consolidated call failed, fall through to the individual client
        if self._exhausted or self._cache is None:
            return self._client(prompt)

        # Serve from cache based on which field this module wants
        cache = self._cache
        if field == "location":
            import json as _json
            return _json.dumps({
                "city":    cache.get("city", ""),
                "region":  cache.get("region", ""),
                "country": cache.get("country", ""),
            })
        elif field == "is_remote":
            return str(cache.get("is_remote", ""))
        elif field == "job_type":
            return str(cache.get("job_type", ""))
        elif field == "education":
            return str(cache.get("education", ""))
        elif field == "skills":
            skills = cache.get("skills", [])
            if isinstance(skills, list):
                import json as _json
                return _json.dumps(skills)
            return str(skills)
        else:
            # Unknown field — fall back to individual call
            logger.debug("BatchedGroqCallable: unknown field for prompt, falling back to individual call")
            return self._client(prompt)
