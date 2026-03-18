"""Classify job rows into company standard tech roles using Gemini.

Key behavior:
- Reads job rows from an input CSV.
- Sends each row to Gemini.
- Maps model output to a canonical role using `standard_tech_roles.py`.
- Writes original rows + target/meta columns to an output CSV.

Environment:
- `GEMINI_API_KEYS` (comma-separated), or `GEMINI_API_KEY_[1..20]`, or `GEMINI_API_KEY`
- Optional `GEMINI_MODEL` (default: `gemini-3-flash-preview`)

`.env` support:
- If keys are not found in process env, this module also reads `.env`.
- Supports list style like:
    `GEMINI_API_KEY=[key1,key2,...]`
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ValidationError

from standard_tech_roles import StandardTechRoles

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class KeyRotator:
    """Thread-safe round-robin API key selector."""

    def __init__(self, keys: List[str]):
        if not keys:
            raise ValueError("KeyRotator requires at least one API key")
        self._keys = keys
        self._lock = threading.Lock()
        self._index = 0

    def next(self) -> str:
        with self._lock:
            key = self._keys[self._index]
            self._index = (self._index + 1) % len(self._keys)
            return key

    def count(self) -> int:
        return len(self._keys)


class JobRow(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    company: Optional[str] = None
    location: Optional[str] = None

    class Config:
        extra = "allow"

    def as_text(self) -> str:
        parts: List[str] = []
        for k, v in self.__dict__.items():
            if v is None or k.startswith("_"):
                continue
            parts.append(f"{k}: {v}")
        return "\n".join(parts)


class GeminiResponse(BaseModel):
    is_tech: bool
    predicted_role: Optional[str] = None
    confidence: float = 0.0
    raw: Optional[Dict[str, Any]] = None


def _parse_key_list(raw: str) -> List[str]:
    """Parse keys from comma/newline-separated text or bracket list text."""
    text = raw.strip()
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    items: List[str] = []
    for piece in text.replace("\n", ",").split(","):
        token = piece.strip().strip("'\"")
        if token:
            items.append(token)
    return items


def _read_dotenv_map(dotenv_path: str) -> Dict[str, str]:
    """Very small .env reader that supports multiline list values."""
    values: Dict[str, str] = {}
    if not os.path.exists(dotenv_path):
        return values

    with open(dotenv_path, encoding="utf-8") as f:
        lines = f.readlines()

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


def _dotenv_candidates() -> List[str]:
    cwd = os.getcwd()
    file_dir = os.path.dirname(os.path.abspath(__file__))
    return [
        os.path.join(cwd, ".env"),
        os.path.join(file_dir, ".env"),
        os.path.join(os.path.dirname(file_dir), ".env"),
    ]


def _load_dotenv_values() -> Dict[str, str]:
    merged: Dict[str, str] = {}
    for path in _dotenv_candidates():
        if os.path.exists(path):
            merged.update(_read_dotenv_map(path))
    return merged


def get_api_keys() -> List[str]:
    """Get API keys from env, with .env fallback and list-format support."""
    keys: List[str] = []

    raw_multi = os.environ.get("GEMINI_API_KEYS")
    if raw_multi:
        keys.extend(_parse_key_list(raw_multi))

    if not keys:
        for i in range(1, 21):
            key_i = os.environ.get(f"GEMINI_API_KEY_{i}")
            if key_i:
                keys.append(key_i.strip())

    if not keys:
        raw_single = os.environ.get("GEMINI_API_KEY")
        if raw_single:
            keys.extend(_parse_key_list(raw_single))

    if keys:
        return keys

    dotenv_values = _load_dotenv_values()
    raw_multi = dotenv_values.get("GEMINI_API_KEYS")
    if raw_multi:
        keys.extend(_parse_key_list(raw_multi))

    if not keys:
        for i in range(1, 21):
            key_i = dotenv_values.get(f"GEMINI_API_KEY_{i}")
            if key_i:
                keys.append(key_i.strip().strip("'\""))

    if not keys:
        raw_single = dotenv_values.get("GEMINI_API_KEY")
        if raw_single:
            keys.extend(_parse_key_list(raw_single))

    # Deduplicate while preserving order
    seen = set()
    unique: List[str] = []
    for k in keys:
        if k not in seen:
            seen.add(k)
            unique.append(k)
    return unique


def get_model_name() -> str:
    model = os.environ.get("GEMINI_MODEL")
    if model:
        return model.strip()
    return _load_dotenv_values().get("GEMINI_MODEL", "gemini-2.5-flash").strip()


def extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    """Best-effort extraction of a JSON object from text."""
    try:
        loaded = json.loads(text)
        return loaded if isinstance(loaded, dict) else None
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    candidate = text[start : end + 1]
    try:
        loaded = json.loads(candidate)
        return loaded if isinstance(loaded, dict) else None
    except Exception:
        return None


def _extract_json_value(text: str) -> Optional[Any]:
    """Best-effort extraction of any JSON value (object/array) from model output."""
    try:
        return json.loads(text)
    except Exception:
        pass

    starts = [i for i in [text.find("{"), text.find("[")] if i != -1]
    if not starts:
        return None
    start = min(starts)

    ends = [i for i in [text.rfind("}"), text.rfind("]")] if i != -1]
    if not ends:
        return None
    end = max(ends)

    if end <= start:
        return None

    try:
        return json.loads(text[start : end + 1])
    except Exception:
        return None


def build_batch_prompt(job_texts: List[str]) -> str:
    """Build a single prompt that classifies many rows in one model call."""
    roles_list = ", ".join(StandardTechRoles.all_canonical())
    parts: List[str] = [
        "You are a classification assistant.",
        "Classify each item as technical/non-technical and map technical ones to one canonical role.",
        "Return ONLY JSON.",
        "Expected output format:",
        '[{"index":0,"is_tech":true,"predicted_role":"Software Engineer","confidence":0.93}]',
        "Use one object per input item index.",
        "If non-technical, set predicted_role to null.",
        f"Canonical roles: {roles_list}",
        "",
        "Input items:",
    ]
    for idx, text in enumerate(job_texts):
        parts.append(f"[{idx}]\n{text}\n")
    return "\n".join(parts)


def _normalize_batch_results(parsed: Any, expected_len: int) -> List[GeminiResponse]:
    """Normalize model JSON into a fixed-length GeminiResponse list."""
    defaults = [
        GeminiResponse(is_tech=False, predicted_role=None, confidence=0.0, raw={"error": "missing_item"})
        for _ in range(expected_len)
    ]

    items: List[Dict[str, Any]] = []
    if isinstance(parsed, list):
        items = [x for x in parsed if isinstance(x, dict)]
    elif isinstance(parsed, dict):
        if isinstance(parsed.get("results"), list):
            items = [x for x in parsed["results"] if isinstance(x, dict)]
        else:
            items = [parsed]

    for i, item in enumerate(items):
        idx_raw = item.get("index", i)
        try:
            idx = int(idx_raw)
        except Exception:
            idx = i
        if idx < 0 or idx >= expected_len:
            continue

        defaults[idx] = GeminiResponse(
            is_tech=bool(item.get("is_tech", False)),
            predicted_role=(item.get("predicted_role") or None),
            confidence=float(item.get("confidence", 0.0) or 0.0),
            raw=item,
        )

    return defaults


def query_gemini_batch(
    job_texts: List[str],
    api_key: str,
    timeout: int = 30,
    retry_attempts: int = 2,
    retry_delay: float = 0.8,
) -> List[GeminiResponse]:
    """Classify multiple jobs in one SDK request with lightweight retries."""
    prompt = build_batch_prompt(job_texts)

    # SDK-only path (no URL fallback by request).
    from google import genai  # type: ignore

    client = genai.Client(api_key=api_key)
    model = get_model_name()

    last_error: Optional[Exception] = None
    total_attempts = max(1, retry_attempts)
    for attempt in range(total_attempts):
        try:
            res = client.models.generate_content(model=model, contents=prompt)
            text = getattr(res, "text", None) or str(res)
            parsed = _extract_json_value(text)
            if parsed is None:
                return [
                    GeminiResponse(
                        is_tech=False,
                        predicted_role=None,
                        confidence=0.0,
                        raw={"error": "invalid_json", "text": text},
                    )
                    for _ in job_texts
                ]
            return _normalize_batch_results(parsed, len(job_texts))
        except Exception as e:
            last_error = e
            if attempt < total_attempts - 1:
                time.sleep(retry_delay * (attempt + 1))

    return [
        GeminiResponse(
            is_tech=False,
            predicted_role=None,
            confidence=0.0,
            raw={"error": "sdk_request_failed", "message": str(last_error) if last_error else "unknown"},
        )
        for _ in job_texts
    ]


def query_gemini(job_text: str, api_key: str, timeout: int = 30) -> GeminiResponse:
    results = query_gemini_batch(
        job_texts=[job_text],
        api_key=api_key,
        timeout=timeout,
    )
    return results[0]


def process_input_csv(
    input_csv: str,
    out_csv: str,
    target_col: str = "target_role",
    sleep: float = 0.2,
    batch_size: int = 5,
    retry_attempts: int = 2,
    retry_delay: float = 0.8,
    limit: Optional[int] = None,
    key_rotator: Optional[KeyRotator] = None,
) -> None:
    """Classify each row and write an enriched CSV.

    `target_col` is created if missing.
    """
    if key_rotator is None:
        raise RuntimeError("key_rotator is required")

    with open(input_csv, newline="", encoding="utf-8") as inf:
        reader = csv.DictReader(inf)
        fieldnames = list(reader.fieldnames or [])
        if target_col not in fieldnames:
            fieldnames.append(target_col)

        meta_fields = ["gemini_is_tech", "gemini_predicted_role", "gemini_confidence", "gemini_raw"]
        for col in meta_fields:
            if col not in fieldnames:
                fieldnames.append(col)

        with open(out_csv, "w", newline="", encoding="utf-8") as outf:
            writer = csv.DictWriter(outf, fieldnames=fieldnames)
            writer.writeheader()

            written = 0
            effective_batch_size = max(1, int(batch_size or 1))
            pending_rows: List[Dict[str, Any]] = []

            def flush_batch(rows_batch: List[Dict[str, Any]]) -> int:
                if not rows_batch:
                    return 0

                parsed_rows: List[JobRow] = []
                job_texts: List[str] = []
                for row_item in rows_batch:
                    try:
                        jr = JobRow.parse_obj(row_item)
                    except ValidationError:
                        jr = JobRow()
                    parsed_rows.append(jr)
                    job_texts.append(jr.as_text() or "\n".join([f"{k}: {v}" for k, v in row_item.items() if v]))

                current_key = key_rotator.next()
                try:
                    responses = query_gemini_batch(
                        job_texts=job_texts,
                        api_key=current_key,
                        retry_attempts=retry_attempts,
                        retry_delay=retry_delay,
                    )
                except Exception as e:
                    logger.warning("Batch request failed for current key: %s", e)
                    responses = [
                        GeminiResponse(is_tech=False, predicted_role=None, confidence=0.0, raw={"error": "batch_failed"})
                        for _ in rows_batch
                    ]

                batch_written = 0
                for row_item, jr, resp in zip(rows_batch, parsed_rows, responses):
                    canonical = None
                    if resp.is_tech and resp.predicted_role:
                        canonical = StandardTechRoles.find_canonical(resp.predicted_role)
                    if canonical is None and jr.title:
                        canonical = StandardTechRoles.find_canonical(jr.title)

                    out_row = dict(row_item)
                    out_row[target_col] = canonical or ""
                    out_row["gemini_is_tech"] = resp.is_tech
                    out_row["gemini_predicted_role"] = resp.predicted_role or ""
                    out_row["gemini_confidence"] = resp.confidence
                    out_row["gemini_raw"] = json.dumps(resp.raw, ensure_ascii=False)
                    writer.writerow(out_row)
                    batch_written += 1

                if sleep > 0:
                    # Sleep between batches to smooth traffic bursts and reduce 429s.
                    time.sleep(sleep)
                return batch_written

            for row in reader:
                if limit is not None and written >= limit:
                    break

                pending_rows.append(row)
                if len(pending_rows) >= effective_batch_size:
                    written += flush_batch(pending_rows)
                    pending_rows = []

            if pending_rows and (limit is None or written < limit):
                written += flush_batch(pending_rows)

    logger.info("Wrote %d rows to %s", written, out_csv)


def classify_csv(
    input_csv: str,
    out_csv: Optional[str] = None,
    target_col: str = "target_role",
    sleep: float = 0.2,
    batch_size: int = 5,
    retry_attempts: int = 2,
    retry_delay: float = 0.8,
    limit: Optional[int] = None,
) -> None:
    """Programmatic entrypoint.

    Reads API keys from environment/.env and rotates them round-robin.
    """
    keys = get_api_keys()
    if not keys:
        raise RuntimeError("No GEMINI API keys found. Set GEMINI_API_KEYS or GEMINI_API_KEY (or .env equivalent).")

    if out_csv is None:
        base, _ = os.path.splitext(input_csv)
        out_csv = f"{base}_labeled.csv"

    rotator = KeyRotator(keys)
    return process_input_csv(
        input_csv=input_csv,
        out_csv=out_csv,
        target_col=target_col,
        sleep=sleep,
        batch_size=batch_size,
        retry_attempts=retry_attempts,
        retry_delay=retry_delay,
        limit=limit,
        key_rotator=rotator,
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", default=None, help="Input CSV to classify")
    p.add_argument("--out", default=None, help="Output CSV path (default: <input>_labeled.csv)")
    p.add_argument("--sleep", type=float, default=0.2)
    p.add_argument("--batch-size", type=int, default=5, help="Rows per Gemini request")
    p.add_argument("--retry-attempts", type=int, default=2, help="SDK attempts per batch")
    p.add_argument("--retry-delay", type=float, default=0.8, help="Base delay between retries (seconds)")
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()

    if not args.input:
        print("Provide --input <input.csv>")
        return

    classify_csv(
        input_csv=args.input,
        out_csv=args.out,
        sleep=args.sleep,
        batch_size=args.batch_size,
        retry_attempts=args.retry_attempts,
        retry_delay=args.retry_delay,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
