"""gemini_role_classifier.py

Uses the `standard_tech_roles` module to map job postings to company-standard
canonical roles. Queries a Gemini-compatible model to determine whether a
job posting is technical and to suggest a standard role. The script is
designed to be imported and used programmatically; a lightweight CLI is also provided.

Environment variables (required):
- `GEMINI_API_URL` — HTTP endpoint for a Gemini-compatible model that accepts
    JSON payloads of the form {"prompt": "..."} and returns text or JSON.
- `GEMINI_API_KEY` — Bearer token used in the `Authorization: Bearer ...`
    header when calling the API.

Defaults and files:
- `out_csv` defaults to `<input_basename>_labeled.csv` and will contain the
    original rows plus a `target_role` column and Gemini metadata columns.

Usage (programmatic):
    from gemini_role_classifier import classify_csv
    classify_csv('unclean_jobs.csv')

Usage (CLI):
    python gemini_role_classifier.py --input unclean_jobs.csv

Note: The Gemini endpoint should ideally return JSON with keys:
`is_tech` (bool), `predicted_role` (str|null), `confidence` (float). If it
returns plain text, the script will attempt to extract JSON embedded in the
text. Adapt `query_gemini()` if your provider returns a different envelope.
"""
from __future__ import annotations
import csv
import json
import os
import re
import time
import argparse
from typing import List, Optional, Dict, Any

import requests
import logging
from pydantic import BaseModel, ValidationError
from standard_tech_roles import StandardTechRoles


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
            if v is None:
                continue
            # skip internal pydantic attributes
            if k.startswith("_"):
                continue
            parts.append(f"{k}: {v}")
        return "\n".join(parts)

# module logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class GeminiResponse(BaseModel):
    is_tech: bool
    predicted_role: Optional[str] = None
    confidence: float = 0.0
    raw: Optional[Dict[str, Any]] = None


def extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    # Try to parse direct JSON first
    try:
        return json.loads(text)
    except Exception:
        pass
    # Fallback: find first {...} block and try to parse
    m = re.search(r"\{(?:[^{}]|(?R))*\}", text)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            return None
    return None


def query_gemini(job_text: str, canonical_options: List[str], api_url: str, api_key: str, timeout: int = 30) -> GeminiResponse:
    prompt = (
        "You are a classifier.\n"
        "Given the job posting or title provided, answer with a JSON object only with keys: is_tech (true/false), predicted_role (one of the canonical roles or null), confidence (0.0-1.0).\n"
        f"Canonical roles: {', '.join(canonical_options)}\n"
        "Respond with JSON only.\n"
        "Input:\n"
        f"{job_text}\n"
    )

    payload = {"prompt": prompt, "max_tokens": 200}
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    resp = None
    attempt = 0
    max_retries = 3
    while attempt < max_retries:
        try:
            resp = requests.post(api_url, headers=headers, json=payload, timeout=timeout)
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            attempt += 1
            wait = 2 ** attempt
            logger.warning("Request to Gemini failed (attempt %d/%d): %s", attempt, max_retries, e)
            time.sleep(wait)
    if resp is None:
        return GeminiResponse(is_tech=False, predicted_role=None, confidence=0.0, raw={"error": "request_failed"})

    text = resp.text
    parsed = None
    # Try if response already JSON-decodes
    try:
        parsed = resp.json()
    except Exception:
        parsed = extract_json_from_text(text)

    if isinstance(parsed, dict):
        # If the API nested the JSON in another envelope, try to find keys
        if "is_tech" in parsed and "confidence" in parsed:
            data = parsed
        else:
            # If provider returned text answer, try to locate fields in nested dicts
            # Otherwise attach raw
            data = parsed
        # Attempt to coerce to GeminiResponse
        try:
            return GeminiResponse(
                is_tech=bool(data.get("is_tech", False)),
                predicted_role=(data.get("predicted_role") or None),
                confidence=float(data.get("confidence", 0.0) or 0.0),
                raw=data,
            )
        except Exception:
            return GeminiResponse(is_tech=False, predicted_role=None, confidence=0.0, raw=data)

    # If no JSON parsed, return raw text in `raw`
    return GeminiResponse(is_tech=False, predicted_role=None, confidence=0.0, raw={"text": text})


def iterate_and_save(*args, **kwargs):
    logger.info("iterate_and_save is deprecated; use classify_csv/process_input_csv instead")


def process_input_csv(input_csv: str, api_url: str, api_key: str, out_csv: str, target_col: str = "target_role", sleep: float = 0.2, limit: Optional[int] = None):
    """Process `input_csv`, classify each row with Gemini and write `out_csv`.

    - If `target_col` does not exist, it will be created.
    - For each row all fields are concatenated and sent to Gemini for classification.
    - If Gemini indicates a tech job, we map its `predicted_role` to a canonical
      role using `StandardTechRoles`. If mapping fails, leave target blank.
    """
    if not api_url or not api_key:
        raise RuntimeError("GEMINI_API_URL and GEMINI_API_KEY must be provided to process_input_csv()")

    canonical_options = StandardTechRoles.all_canonical()

    with open(input_csv, newline="", encoding="utf-8") as inf:
        reader = csv.DictReader(inf)
        fieldnames = list(reader.fieldnames or [])
        if target_col not in fieldnames:
            fieldnames.append(target_col)
        # Add Gemini metadata fields
        meta_fields = ["gemini_is_tech", "gemini_predicted_role", "gemini_confidence", "gemini_raw"]
        for mf in meta_fields:
            if mf not in fieldnames:
                fieldnames.append(mf)

        with open(out_csv, "w", newline="", encoding="utf-8") as outf:
            writer = csv.DictWriter(outf, fieldnames=fieldnames)
            writer.writeheader()

            written = 0
            for row in reader:
                if limit is not None and written >= limit:
                    break

                # Validate row with Pydantic (allows extra fields)
                try:
                    jr = JobRow.parse_obj(row)
                except ValidationError:
                    jr = JobRow()

                job_text = jr.as_text() or "\n".join([f"{k}: {v}" for k, v in row.items() if v])

                resp = query_gemini(job_text, canonical_options, api_url, api_key)

                canonical = None
                if resp.is_tech and resp.predicted_role:
                    # Try to map gemini's predicted role to our canonical names
                    canonical = StandardTechRoles.find_canonical(resp.predicted_role)
                    # If not found, also try mapping directly on the predicted text
                    if canonical is None:
                        canonical = StandardTechRoles.find_canonical(resp.predicted_role.lower())

                # As a fallback, try to map from job title if available
                if canonical is None and jr.title:
                    canonical = StandardTechRoles.find_canonical(jr.title)

                out_row = dict(row)
                out_row[target_col] = canonical or ""
                out_row["gemini_is_tech"] = resp.is_tech
                out_row["gemini_predicted_role"] = resp.predicted_role or ""
                out_row["gemini_confidence"] = resp.confidence
                out_row["gemini_raw"] = json.dumps(resp.raw, ensure_ascii=False)

                writer.writerow(out_row)
                written += 1
                time.sleep(sleep)

    logger.info("Wrote %d rows to %s", written, out_csv)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default=None, help="Input CSV to classify (if provided, will run classification)")
    p.add_argument("--out", default=None, help="Output CSV path. Defaults to <input_basename>_labeled.csv")
    p.add_argument("--sleep", type=float, default=0.2)
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()
    # Credentials are read from environment variables: GEMINI_API_URL, GEMINI_API_KEY
    if args.input:
        out = args.out
        if out is None:
            base, _ = os.path.splitext(args.input)
            out = f"{base}_labeled.csv"
        classify_csv(input_csv=args.input, out_csv=out, sleep=args.sleep, limit=args.limit)
    else:
        print("No --input provided. To run classification from CLI pass --input <input.csv>.\n")
        print("This module is importable; call classify_csv(input_csv, out_csv) from your code.")


def classify_csv(input_csv: str, out_csv: Optional[str] = None, target_col: str = "target_role", sleep: float = 0.2, limit: Optional[int] = None):
    """Programmatic entrypoint to classify an input CSV.

    - Reads `GEMINI_API_URL` and `GEMINI_API_KEY` from the environment.
    - If `out_csv` is not provided, uses `<input_basename>_labeled.csv`.

    This function is safe to import and call from other Python code (no argparse).
    """
    api_url = os.environ.get("GEMINI_API_URL")
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_url or not api_key:
        raise RuntimeError("GEMINI_API_URL and GEMINI_API_KEY must be set in the environment before calling classify_csv().")

    if out_csv is None:
        base, _ = os.path.splitext(input_csv)
        out_csv = f"{base}_labeled.csv"

    return process_input_csv(input_csv=input_csv, api_url=api_url, api_key=api_key, out_csv=out_csv, target_col=target_col, sleep=sleep, limit=limit)


if __name__ == "__main__":
    main()
