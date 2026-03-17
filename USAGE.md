# gemini_role_classifier — Usage

This repository contains two Python files used to classify job postings into a company-standard set of technical roles:

- `standard_tech_roles.py` — canonical role names and alias-matching helper `StandardTechRoles`.
- `gemini_role_classifier.py` — programmatic and CLI entrypoints that call a Gemini-compatible model to determine whether a job is technical and map it to a canonical role.

## Requirements

- Python 3.8+
- Install runtime deps:

```bash
pip install requests pydantic
```

## Environment

Set these environment variables before running (or provide them to `process_input_csv`):

- `GEMINI_API_URL` — URL of your Gemini-compatible HTTP endpoint (expects JSON input `{ "prompt": "..." }`).
- `GEMINI_API_KEY` — Bearer token for authorization.

## Output

When run, the classifier writes an output CSV that contains all original columns plus:

- `target_role` — mapped canonical role (empty if unknown)
- `gemini_is_tech` — boolean
- `gemini_predicted_role` — the model's suggested role (raw)
- `gemini_confidence` — numeric confidence (0.0-1.0)
- `gemini_raw` — raw response payload (JSON/text)

## Programmatic usage

Recommended for automation. The module exposes two functions:

- `classify_csv(input_csv, out_csv=None, target_col='target_role', sleep=0.2, limit=None)`
  - Reads `GEMINI_API_URL` and `GEMINI_API_KEY` from the environment.
  - `out_csv` defaults to `<input_basename>_labeled.csv` if not provided.

Example (simple):

```python
import os
from gemini_role_classifier import classify_csv

os.environ['GEMINI_API_URL'] = 'https://api.example.com/v1/generate'
os.environ['GEMINI_API_KEY'] = 'your_api_key_here'

classify_csv('unclean_jobs.csv')  # writes unclean_jobs_labeled.csv
```

- `process_input_csv(input_csv, api_url, api_key, out_csv, target_col='target_role', sleep=0.2, limit=None)`
  - Lower-level function; accepts `api_url` and `api_key` explicitly instead of reading env vars.
  - Use this if you want to pass credentials programmatically rather than using env variables.

Example (explicit credentials):

```python
from gemini_role_classifier import process_input_csv

process_input_csv(
    input_csv='unclean_jobs.csv',
    api_url='https://api.example.com/v1/generate',
    api_key='your_api_key_here',
    out_csv='unclean_jobs_labeled.csv'
)
```

## CLI usage

Basic CLI (reads env vars for credentials):

```bash
python gemini_role_classifier.py --input unclean_jobs.csv
```

Specify output path:

```bash
python gemini_role_classifier.py --input unclean_jobs.csv --out cleaned_with_target.csv
```

## Notes and tips

- The classifier uses `standard_tech_roles.py` to map model outputs to canonical roles. If mapping fails, the `target_role` will be empty.
- The Gemini endpoint should ideally return JSON of the form `{ "is_tech": true|false, "predicted_role": "...", "confidence": 0.0 }`. The script will attempt to extract JSON from plain-text responses when possible.
- The script includes a small retry/backoff for transient HTTP errors. Tune `sleep` and `limit` parameters as needed for rate limits.
- For testing, you can mock the HTTP endpoint or run a local server that returns the expected JSON envelope.

If you want, I can add a `requirements.txt`, unit tests with a mocked Gemini server, or a CI-friendly test harness. Which would you prefer next?

## What to call and expected outputs

- Programmatic: call `classify_csv(input_csv, out_csv=None, target_col='target_role', sleep=0.2, limit=None)`.
  - Reads `GEMINI_API_URL` and `GEMINI_API_KEY` from the environment and writes `out_csv` (defaults to `<input_basename>_labeled.csv`).
- Lower-level: call `process_input_csv(input_csv, api_url, api_key, out_csv, ...)` if you want to provide credentials directly.
- CLI: `python gemini_role_classifier.py --input unclean_jobs.csv` (reads credentials from env).

Output CSV: the same columns as your input plus these appended columns:

- `target_role` — the mapped canonical role (empty string if unknown).
- `gemini_is_tech` — boolean: whether the model judged it a tech job.
- `gemini_predicted_role` — the raw role name the model suggested.
- `gemini_confidence` — numeric confidence from the model (0.0–1.0 if provided).
- `gemini_raw` — full raw response (JSON or text) for auditing.

## What happens internally (step-by-step)

1. `classify_csv()` reads `GEMINI_API_URL` and `GEMINI_API_KEY` from the environment (or you pass them to `process_input_csv`).
2. The input CSV is read row-by-row using `csv.DictReader`.
3. Each row is validated/normalized into a `JobRow` (Pydantic). `JobRow.as_text()` concatenates available fields into a single text blob.
4. The script calls `query_gemini(job_text, canonical_options, api_url, api_key)` which builds a prompt (including the list of canonical roles) and sends it to the model endpoint. A small retry/backoff is used for transient HTTP failures.
5. The model response is parsed. The code expects JSON with keys: `is_tech`, `predicted_role`, `confidence`. If the model returns plain text containing JSON, the script will attempt to extract it.
6. If `is_tech` is true and `predicted_role` is present, the script attempts to map that predicted role to one of the canonical roles using `StandardTechRoles.find_canonical()`. If that mapping fails, the script falls back to matching the job `title` to a canonical role.
7. The script writes the original row out to the output CSV with the additional `target_role` and Gemini metadata columns described above.

## Prompt helper

`standard_tech_roles.py` exposes a `build_prompt(job_text)` helper that returns a well-formed prompt containing a system instruction and the full canonical roles list. You can call this directly if you want to construct custom requests to the model. The `query_gemini()` function already assembles a prompt that includes the canonical roles; using `build_prompt()` is optional.

## GEMINI_API_URL — what is it?

`GEMINI_API_URL` is the HTTP endpoint of your model service (for example `https://api.your-model-host/v1/generate`). It is not the prompt. The prompt is the text payload sent in the JSON body (the code sends `{ "prompt": "..." }`).

If you want, I can update `query_gemini()` to use `StandardTechRoles.build_prompt()` directly so the same prompt template is used everywhere. Would you like that?