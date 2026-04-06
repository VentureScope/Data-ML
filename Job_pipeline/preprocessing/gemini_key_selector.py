"""Utilities for selecting Gemini API keys from environment configuration.

Selection strategy (requested behavior):
1. Load available keys from process env and/or .env file.
2. Seed a random number generator from current time.
3. Generate a random index and pick a key using that index.
"""

from __future__ import annotations

import logging
import os
import random
import time
from pathlib import Path
from typing import Dict, List


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


def _read_dotenv_map(dotenv_path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not dotenv_path.exists():
        return values

    lines = dotenv_path.read_text(encoding="utf-8").splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        i += 1

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if value.startswith("[") and not value.endswith("]"):
            chunks = [value]
            while i < len(lines):
                nxt = lines[i].strip()
                i += 1
                chunks.append(nxt)
                if nxt.endswith("]"):
                    break
            value = " ".join(chunks)

        values[key] = value

    return values


def _dotenv_candidates() -> List[Path]:
    cwd = Path.cwd()
    module_dir = Path(__file__).resolve().parent
    return [
        cwd / ".env",
        module_dir / ".env",
        module_dir.parent / ".env",
        module_dir.parent.parent / ".env",
    ]


def _load_dotenv_values() -> Dict[str, str]:
    merged: Dict[str, str] = {}
    for path in _dotenv_candidates():
        if path.exists():
            merged.update(_read_dotenv_map(path))
    return merged


def get_all_gemini_api_keys() -> List[str]:
    """Collect Gemini keys from env variables and .env file."""
    keys: List[str] = []

    raw_multi = os.environ.get("GEMINI_API_KEYS")
    if raw_multi:
        keys.extend(_parse_key_list(raw_multi))

    for i in range(1, 201):
        key_i = os.environ.get(f"GEMINI_API_KEY_{i}")
        if key_i:
            keys.append(key_i.strip())

    raw_single = os.environ.get("GEMINI_API_KEY")
    if raw_single:
        keys.extend(_parse_key_list(raw_single))

    dotenv_values = _load_dotenv_values()
    raw_multi = dotenv_values.get("GEMINI_API_KEYS")
    if raw_multi:
        keys.extend(_parse_key_list(raw_multi))

    for i in range(1, 201):
        key_i = dotenv_values.get(f"GEMINI_API_KEY_{i}")
        if key_i:
            keys.append(key_i.strip().strip("'\""))

    raw_single = dotenv_values.get("GEMINI_API_KEY")
    if raw_single:
        keys.extend(_parse_key_list(raw_single))

    deduped: List[str] = []
    seen = set()
    for key in keys:
        if key and key not in seen:
            seen.add(key)
            deduped.append(key)
    logger.debug("Discovered %d unique Gemini API keys", len(deduped))
    return deduped


def select_random_gemini_api_key() -> str | None:
    """Pick one Gemini API key using time-seeded random index."""
    keys = get_all_gemini_api_keys()
    if not keys:
        logger.debug("No Gemini API keys available")
        return None

    seed = time.time_ns()
    rng = random.Random(seed)
    random_index = rng.randint(0, len(keys) - 1)
    logger.debug("Selected Gemini API key index=%d out_of=%d", random_index, len(keys))
    return keys[random_index]


__all__ = ["get_all_gemini_api_keys", "select_random_gemini_api_key"]
