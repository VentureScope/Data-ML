"""Groq LLM client with multi-key rotation and exponential backoff.

Drop-in replacement for RobustGeminiClient — same __call__(prompt) -> Optional[str] interface.
"""
from __future__ import annotations

import logging
import os
import random
import time
from pathlib import Path
from typing import Dict, List, Optional

from groq import Groq, RateLimitError, APIStatusError

logger = logging.getLogger(__name__)


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
    cwd = Path.cwd()
    module_dir = Path(__file__).resolve().parent
    for p in [cwd / ".env", module_dir / ".env", module_dir.parent / ".env", module_dir.parent.parent / ".env"]:
        merged.update(_read_dotenv(p))
    return merged


def get_all_groq_api_keys() -> List[str]:
    keys: List[str] = []
    dotenv = _load_dotenv()

    for src in (os.environ, dotenv):
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


class RobustGroqClient:
    """Callable LLM client backed by Groq with key rotation and backoff."""

    DEFAULT_MODEL = "llama-3.3-70b-versatile"

    def __init__(self, model: Optional[str] = None):
        self.model = model or os.environ.get("GROQ_MODEL", self.DEFAULT_MODEL)
        self.keys = get_all_groq_api_keys()
        if not self.keys:
            logger.warning("No Groq API keys found. LLM fallback will fail immediately.")
        self.key_backoffs: Dict[str, float] = {k: 0.0 for k in self.keys}
        self.key_failures: Dict[str, int] = {k: 0 for k in self.keys}

    def _get_available_key(self) -> Optional[str]:
        now = time.time()
        available = [k for k in self.keys if self.key_backoffs[k] <= now]
        return random.choice(available) if available else None

    def _get_soonest_available_time(self) -> float:
        return min(self.key_backoffs.values()) if self.keys else time.time()

    def _mark_failed(self, key: str, status_code: Optional[int] = None):
        self.key_failures[key] += 1
        failures = self.key_failures[key]
        base_delay = 60 if status_code == 429 else 10
        jitter = random.uniform(0.8, 1.2)
        delay = min(base_delay * (2 ** min(failures - 1, 5)) * jitter, 600)
        self.key_backoffs[key] = time.time() + delay
        logger.warning(
            "Groq key ...%s failed (status=%s). Backing off %.2fs (failures=%d)",
            key[-4:], status_code, delay, failures,
        )

    def _mark_success(self, key: str):
        self.key_failures[key] = max(0, self.key_failures[key] - 1)

    def __call__(self, prompt: str) -> Optional[str]:
        if not self.keys:
            return None

        max_attempts = 10
        attempt = 0
        while attempt < max_attempts:
            key = self._get_available_key()
            if not key:
                sleep_time = min(max(0.1, self._get_soonest_available_time() - time.time()), 10.0)
                logger.info("All Groq keys rate-limited. Sleeping %.2fs...", sleep_time)
                time.sleep(sleep_time)
                continue

            attempt += 1
            try:
                client = Groq(api_key=key)
                response = client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                    max_tokens=512,
                )
                self._mark_success(key)
                return (response.choices[0].message.content or "").strip()

            except RateLimitError:
                logger.info("Groq rate limited (429) on key ...%s", key[-4:])
                self._mark_failed(key, status_code=429)

            except APIStatusError as e:
                code = e.status_code
                if code in (500, 502, 503, 504):
                    logger.warning("Groq server error (%d) on key ...%s", code, key[-4:])
                    self._mark_failed(key, status_code=code)
                else:
                    logger.error("Unrecoverable Groq API error: %s", e)
                    return None

            except Exception as e:
                err = str(e).lower()
                if "429" in err or "rate" in err:
                    self._mark_failed(key, status_code=429)
                else:
                    logger.exception("Unexpected Groq error on key ...%s: %s", key[-4:], e)
                    self._mark_failed(key)

        logger.error("Groq: failed after %d attempts.", max_attempts)
        return None
